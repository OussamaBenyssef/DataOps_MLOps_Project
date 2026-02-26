"""
DAG Airflow — Alertes & Monitoring Crypto (P1)
================================================
Surveille la qualité des données et les anomalies de marché.
Génère des alertes dans les logs Airflow et MongoDB.

Checks:
  1. Fraîcheur des données (data staleness)
  2. Anomalies de marché (price/volume spikes, RSI extrêmes)
  3. Couverture des indicateurs techniques
  4. Santé du pipeline (gaps, doublons)

Schedule: Toutes les heures (@hourly)
"""

from datetime import datetime, timedelta, timezone
import json

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

# ─────────────────────── CONFIG ──────────────────────────────────────────

MONGODB_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

# Seuils d'alerte
ALERT_THRESHOLDS = {
    "max_data_age_minutes": 10,          # Données considérées stale après 10 min
    "min_coverage_pct": 95.0,            # Couverture minimale acceptable
    "price_spike_pct": 5.0,              # Variation prix > 5% = alerte
    "volume_spike_multiplier": 5.0,      # Volume > 5x moyenne = alerte
    "rsi_oversold": 20,                  # RSI < 20 = alerte
    "rsi_overbought": 80,               # RSI > 80 = alerte
    "max_null_rate_pct": 1.0,            # > 1% nulls = alerte
    "max_duplicates": 0,                 # Doublons pas tolérés
}

default_args = {
    "owner": "P1-Chef-de-Projet",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
}


# ─────────────────────── TASK FUNCTIONS ──────────────────────────────────

def check_data_freshness(**context):
    """
    Tâche 1: Vérifie la fraîcheur des données OHLCV dans MongoDB.
    Alerte si les données les plus récentes sont trop anciennes.
    """
    import logging
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]
    ohlcv = db["ohlcv"]

    alerts = []
    freshness = {}
    now = datetime.now(timezone.utc)
    max_age = ALERT_THRESHOLDS["max_data_age_minutes"]

    for symbol in TRADING_PAIRS:
        # Trouver le document le plus récent
        latest = ohlcv.find_one(
            {"symbol": symbol},
            sort=[("timestamp", -1)]
        )

        if latest is None:
            alerts.append({
                "type": "data_missing",
                "severity": "critical",
                "symbol": symbol,
                "message": f"Aucune donnée OHLCV pour {symbol}",
            })
            freshness[symbol] = {"status": "MISSING", "age_minutes": None}
            continue

        # Calculer l'âge
        ts = latest.get("timestamp", 0)
        if isinstance(ts, (int, float)):
            latest_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        else:
            latest_dt = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts

        age_minutes = (now - latest_dt).total_seconds() / 60

        freshness[symbol] = {
            "status": "STALE" if age_minutes > max_age else "FRESH",
            "age_minutes": round(age_minutes, 1),
            "latest_timestamp": str(latest_dt),
        }

        if age_minutes > max_age:
            alerts.append({
                "type": "data_stale",
                "severity": "high" if age_minutes > max_age * 3 else "medium",
                "symbol": symbol,
                "message": f"{symbol} dernière donnée il y a {age_minutes:.0f} min (seuil: {max_age} min)",
            })
            logger.warning(f"⚠️  {symbol} STALE — {age_minutes:.0f} min")
        else:
            logger.info(f"✅ {symbol} FRESH — {age_minutes:.1f} min")

    client.close()

    context["ti"].xcom_push(key="freshness", value=freshness)
    context["ti"].xcom_push(key="freshness_alerts", value=alerts)
    return {"freshness": freshness, "alerts_count": len(alerts)}


def detect_market_anomalies(**context):
    """
    Tâche 2: Détecte les anomalies de marché dans les dernières données.
    Vérifie: price spikes, volume spikes, RSI extrêmes, Bollinger breakouts.
    """
    import logging
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]
    ohlcv = db["ohlcv"]
    indicators = db["indicators"]
    anomalies_coll = db["anomalies"]

    alerts = []
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=1)

    for symbol in TRADING_PAIRS:
        logger.info(f"Analyse anomalies {symbol}...")

        # 1. Price spikes — dernières bougies
        recent_candles = list(ohlcv.find(
            {"symbol": symbol, "timestamp": {"$gte": cutoff_dt}},
            sort=[("timestamp", -1)],
        ).limit(60))

        if len(recent_candles) >= 2:
            for i in range(len(recent_candles) - 1):
                curr = recent_candles[i]
                prev = recent_candles[i + 1]
                if prev.get("close") and curr.get("close") and prev["close"] > 0:
                    change_pct = abs((curr["close"] - prev["close"]) / prev["close"] * 100)
                    if change_pct >= ALERT_THRESHOLDS["price_spike_pct"]:
                        alert = {
                            "type": "price_spike",
                            "severity": "critical" if change_pct > 10 else "high",
                            "symbol": symbol,
                            "message": f"Price spike {symbol}: {change_pct:.2f}% en 1 bougie",
                            "value": change_pct,
                        }
                        alerts.append(alert)
                        logger.warning(f"🚨 {alert['message']}")

        # 2. Volume spikes
        if len(recent_candles) >= 20:
            volumes = [c.get("volume", 0) for c in recent_candles if c.get("volume")]
            if volumes:
                avg_vol = sum(volumes) / len(volumes)
                latest_vol = volumes[0]
                if avg_vol > 0 and latest_vol > avg_vol * ALERT_THRESHOLDS["volume_spike_multiplier"]:
                    alert = {
                        "type": "volume_spike",
                        "severity": "high",
                        "symbol": symbol,
                        "message": f"Volume spike {symbol}: {latest_vol:.0f} vs avg {avg_vol:.0f} ({latest_vol/avg_vol:.1f}x)",
                        "value": latest_vol / avg_vol,
                    }
                    alerts.append(alert)
                    logger.warning(f"📊 {alert['message']}")

        # 3. RSI extrêmes
        latest_indicator = indicators.find_one(
            {"symbol": symbol},
            sort=[("timestamp", -1)]
        )
        if latest_indicator and latest_indicator.get("rsi_14") is not None:
            rsi = latest_indicator["rsi_14"]
            if rsi < ALERT_THRESHOLDS["rsi_oversold"]:
                alerts.append({
                    "type": "rsi_extreme",
                    "severity": "medium",
                    "symbol": symbol,
                    "message": f"RSI survente {symbol}: {rsi:.1f} (seuil: {ALERT_THRESHOLDS['rsi_oversold']})",
                    "value": rsi,
                })
                logger.warning(f"📉 RSI survente {symbol}: {rsi:.1f}")
            elif rsi > ALERT_THRESHOLDS["rsi_overbought"]:
                alerts.append({
                    "type": "rsi_extreme",
                    "severity": "medium",
                    "symbol": symbol,
                    "message": f"RSI surachat {symbol}: {rsi:.1f} (seuil: {ALERT_THRESHOLDS['rsi_overbought']})",
                    "value": rsi,
                })
                logger.warning(f"📈 RSI surachat {symbol}: {rsi:.1f}")

    # Stocker les anomalies critiques dans MongoDB
    now = datetime.now(timezone.utc)
    for alert in alerts:
        if alert["severity"] in ("high", "critical"):
            try:
                anomalies_coll.insert_one({
                    "symbol": alert["symbol"],
                    "anomaly_type": alert["type"],
                    "severity": alert["severity"],
                    "detected_at": now,
                    "value": alert.get("value", 0.0),
                    "description": alert["message"],
                    "interval": "1m",
                    "metadata": {"source": "airflow_alertes"},
                })
            except Exception as e:
                logger.error(f"Erreur insertion anomalie: {e}")

    client.close()

    logger.info(f"Anomalies détectées: {len(alerts)}")
    context["ti"].xcom_push(key="market_alerts", value=alerts)
    return {"alerts_count": len(alerts), "alerts": alerts}


def check_pipeline_health(**context):
    """
    Tâche 3: Vérifie la santé du pipeline ETL.
    Contrôle: nulls, doublons, couverture indicateurs.
    """
    import logging
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]
    ohlcv = db["ohlcv"]
    indicators = db["indicators"]

    alerts = []
    health = {}
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=1)

    for symbol in TRADING_PAIRS:
        # 1. Vérifier les nulls dans les données récentes
        recent = list(ohlcv.find(
            {"symbol": symbol, "timestamp": {"$gte": cutoff_dt}},
        ).limit(100))

        if recent:
            required = ["open", "high", "low", "close", "volume"]
            total_fields = len(recent) * len(required)
            null_count = sum(
                1 for doc in recent for f in required if doc.get(f) is None
            )
            null_rate = (null_count / total_fields * 100) if total_fields > 0 else 0

            if null_rate > ALERT_THRESHOLDS["max_null_rate_pct"]:
                alerts.append({
                    "type": "null_rate_high",
                    "severity": "high",
                    "symbol": symbol,
                    "message": f"Null rate {symbol}: {null_rate:.2f}% (seuil: {ALERT_THRESHOLDS['max_null_rate_pct']}%)",
                })

            health[f"{symbol}_null_rate"] = round(null_rate, 2)
        else:
            health[f"{symbol}_null_rate"] = None

        # 2. Vérifier les doublons
        pipeline_agg = [
            {"$match": {"symbol": symbol, "timestamp": {"$gte": cutoff_dt}}},
            {"$group": {"_id": {"symbol": "$symbol", "interval": "$interval", "timestamp": "$timestamp"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "duplicates"},
        ]
        dup_result = list(ohlcv.aggregate(pipeline_agg))
        dup_count = dup_result[0]["duplicates"] if dup_result else 0

        if dup_count > ALERT_THRESHOLDS["max_duplicates"]:
            alerts.append({
                "type": "duplicates",
                "severity": "medium",
                "symbol": symbol,
                "message": f"Doublons détectés {symbol}: {dup_count}",
            })

        health[f"{symbol}_duplicates"] = dup_count

        # 3. Couverture indicateurs vs OHLCV
        ohlcv_count = ohlcv.count_documents({"symbol": symbol, "timestamp": {"$gte": cutoff_dt}})
        ind_count = indicators.count_documents({"symbol": symbol, "timestamp": {"$gte": cutoff_dt}})

        if ohlcv_count > 0:
            coverage = round(ind_count / ohlcv_count * 100, 1)
            health[f"{symbol}_indicator_coverage"] = coverage
            if coverage < ALERT_THRESHOLDS["min_coverage_pct"]:
                alerts.append({
                    "type": "low_coverage",
                    "severity": "medium",
                    "symbol": symbol,
                    "message": f"Couverture indicateurs {symbol}: {coverage}% (seuil: {ALERT_THRESHOLDS['min_coverage_pct']}%)",
                })

    client.close()

    logger.info(f"Santé pipeline: {json.dumps(health, indent=2, default=str)}")
    context["ti"].xcom_push(key="health", value=health)
    context["ti"].xcom_push(key="health_alerts", value=alerts)
    return {"health": health, "alerts_count": len(alerts)}


def decide_alert_level(**context):
    """
    Tâche de branchement: décide si on génère un rapport critique ou normal.
    """
    ti = context["ti"]

    freshness_alerts = ti.xcom_pull(task_ids="check_data_freshness", key="freshness_alerts") or []
    market_alerts = ti.xcom_pull(task_ids="detect_market_anomalies", key="market_alerts") or []
    health_alerts = ti.xcom_pull(task_ids="check_pipeline_health", key="health_alerts") or []

    all_alerts = freshness_alerts + market_alerts + health_alerts
    critical = [a for a in all_alerts if a.get("severity") == "critical"]

    if critical:
        return "generate_critical_report"
    return "generate_normal_report"


def generate_critical_report(**context):
    """
    Rapport critique — alertes sévères détectées.
    """
    import logging

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    all_alerts = (
        (ti.xcom_pull(task_ids="check_data_freshness", key="freshness_alerts") or []) +
        (ti.xcom_pull(task_ids="detect_market_anomalies", key="market_alerts") or []) +
        (ti.xcom_pull(task_ids="check_pipeline_health", key="health_alerts") or [])
    )

    report = [
        "🚨" * 20,
        "🚨 RAPPORT CRITIQUE — ALERTES CRYPTO",
        "🚨" * 20,
        f"Timestamp: {datetime.now(timezone.utc).isoformat()}Z",
        f"Total alertes: {len(all_alerts)}",
        "",
    ]

    for severity in ["critical", "high", "medium", "low"]:
        filtered = [a for a in all_alerts if a.get("severity") == severity]
        if filtered:
            report.append(f"--- {severity.upper()} ({len(filtered)}) ---")
            for a in filtered:
                report.append(f"  [{a['type']}] {a['message']}")
            report.append("")

    logger.critical("\n".join(report))
    return "\n".join(report)


def generate_normal_report(**context):
    """
    Rapport normal — aucune alerte critique.
    """
    import logging

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    freshness = ti.xcom_pull(task_ids="check_data_freshness", key="freshness") or {}
    health = ti.xcom_pull(task_ids="check_pipeline_health", key="health") or {}
    all_alerts = (
        (ti.xcom_pull(task_ids="check_data_freshness", key="freshness_alerts") or []) +
        (ti.xcom_pull(task_ids="detect_market_anomalies", key="market_alerts") or []) +
        (ti.xcom_pull(task_ids="check_pipeline_health", key="health_alerts") or [])
    )

    report = [
        "=" * 50,
        "✅ MONITORING CRYPTO — Rapport horaire",
        "=" * 50,
        f"Timestamp: {datetime.now(timezone.utc).isoformat()}Z",
        "",
        "📡 Fraîcheur:",
    ]
    for symbol, info in freshness.items():
        status = info.get("status", "?")
        age = info.get("age_minutes", "?")
        report.append(f"  {symbol}: {status} ({age} min)")

    if all_alerts:
        report.append(f"\n⚠️  Alertes non-critiques: {len(all_alerts)}")
        for a in all_alerts[:10]:
            report.append(f"  [{a['severity']}] {a['message']}")
    else:
        report.append("\n✅ Aucune alerte")

    report.extend(["", "=" * 50])
    logger.info("\n".join(report))
    return "\n".join(report)


# ─────────────────────── DAG ALERTES ─────────────────────────────────────

with DAG(
    dag_id="crypto_alertes",
    default_args=default_args,
    description="Monitoring horaire: fraîcheur données, anomalies marché, santé pipeline",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "alertes", "monitoring", "P1"],
) as dag_alertes:

    t_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
    )

    t_anomalies = PythonOperator(
        task_id="detect_market_anomalies",
        python_callable=detect_market_anomalies,
        provide_context=True,
    )

    t_health = PythonOperator(
        task_id="check_pipeline_health",
        python_callable=check_pipeline_health,
        provide_context=True,
    )

    t_decide = BranchPythonOperator(
        task_id="decide_alert_level",
        python_callable=decide_alert_level,
        provide_context=True,
    )

    t_critical = PythonOperator(
        task_id="generate_critical_report",
        python_callable=generate_critical_report,
        provide_context=True,
    )

    t_normal = PythonOperator(
        task_id="generate_normal_report",
        python_callable=generate_normal_report,
        provide_context=True,
    )

    # Flux parallèle pour les 3 checks, puis branchement
    [t_freshness, t_anomalies, t_health] >> t_decide
    t_decide >> [t_critical, t_normal]
