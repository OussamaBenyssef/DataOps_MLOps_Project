"""
DAG Airflow — Pipeline ML Training Crypto (P4)
================================================
Orchestre l'entraînement automatique des modèles ML :
  1. Vérification des nouvelles données depuis le dernier training
  2. Training XGBoost + LSTM (prédiction prix) — sur TOUTES les données
  3. Training Isolation Forest (détection anomalies) — sur TOUTES les données
  4. Détection de drift (données anciennes vs nouvelles)
  5. Enregistrement des meilleurs modèles dans MLflow Registry
  6. Mise à jour du marqueur de dernier entraînement
  7. Résumé du pipeline

Logique:
  - Le DAG tourne @daily
  - check_new_data compte les docs OHLCV arrivés DEPUIS le dernier training
  - Si nouvelles données < MIN_NEW_SAMPLES → skip (ShortCircuitOperator)
  - Si ok → entraîne sur TOUTE la collection OHLCV
  - Après training → met à jour le marqueur dans ml_metadata

Schedule: @daily
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

# ─────────────────────── CONFIG ──────────────────────────────────────────

MONGODB_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
MLFLOW_TRACKING_URI = "http://mlflow:5000"
TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
MIN_NEW_SAMPLES = 500  # Minimum de nouvelles données pour déclencher le training

default_args = {
    "owner": "P4-ML-Engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ─────────────────────── TASK FUNCTIONS ──────────────────────────────────

def check_new_data(**context):
    """
    Tâche 1 (ShortCircuit): Vérifie s'il y a assez de NOUVELLES données
    depuis le dernier entraînement.

    - Lit le timestamp du dernier training depuis ml_metadata
    - Compte les nouvelles lignes OHLCV depuis ce timestamp
    - Retourne True (continuer) ou False (skip le pipeline)
    """
    import logging
    from datetime import datetime, timezone
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]

    # Récupérer le marqueur du dernier entraînement
    metadata = db["ml_metadata"].find_one({"_id": "last_training"})
    if metadata and metadata.get("timestamp"):
        last_training_ts = metadata["timestamp"]
        if last_training_ts.tzinfo is None:
            last_training_ts = last_training_ts.replace(tzinfo=timezone.utc)
        logger.info(f"Dernier entraînement: {last_training_ts.isoformat()}")
    else:
        # Premier entraînement — pas de marqueur → on considère tout comme nouveau
        last_training_ts = datetime(2000, 1, 1, tzinfo=timezone.utc)
        logger.info("Aucun entraînement précédent trouvé — premier run")

    # Compter les nouvelles données par symbole depuis le dernier training
    total_new = 0
    new_data_stats = {}
    for symbol in TRADING_PAIRS:
        count = db["ohlcv"].count_documents({
            "symbol": symbol,
            "timestamp": {"$gt": last_training_ts},
        })
        new_data_stats[symbol] = count
        total_new += count
        logger.info(f"  {symbol}: {count} nouvelles lignes")

    # Données totales (pour info)
    total_all = db["ohlcv"].count_documents({})
    logger.info(f"Total nouvelles données: {total_new} (seuil: {MIN_NEW_SAMPLES})")
    logger.info(f"Total données complètes: {total_all}")

    client.close()

    # Stocker les stats pour les tâches suivantes
    context["ti"].xcom_push(key="new_data_stats", value=new_data_stats)
    context["ti"].xcom_push(key="total_new", value=total_new)
    context["ti"].xcom_push(key="total_all", value=total_all)
    context["ti"].xcom_push(key="last_training_ts", value=str(last_training_ts))

    # ShortCircuit: True → continuer, False → skip
    if total_new >= MIN_NEW_SAMPLES:
        logger.info(f"✅ Assez de nouvelles données ({total_new} >= {MIN_NEW_SAMPLES}) — lancement du training")
        return True
    else:
        logger.info(f"⏭️  Pas assez de nouvelles données ({total_new} < {MIN_NEW_SAMPLES}) — skip training")
        return False


def update_training_marker(**context):
    """
    Tâche 6: Met à jour le marqueur du dernier entraînement dans MongoDB.
    Permet au prochain run de ne compter que les NOUVELLES données.
    """
    import logging
    from datetime import datetime, timezone
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]

    now = datetime.now(timezone.utc)
    db["ml_metadata"].update_one(
        {"_id": "last_training"},
        {"$set": {
            "timestamp": now,
            "execution_date": str(context.get("execution_date", "")),
            "total_new_data": context["ti"].xcom_pull(
                task_ids="check_new_data", key="total_new"
            ),
            "total_all_data": context["ti"].xcom_pull(
                task_ids="check_new_data", key="total_all"
            ),
        }},
        upsert=True,
    )
    client.close()

    logger.info(f"✅ Marqueur mis à jour: {now.isoformat()}")
    context["ti"].xcom_push(key="training_timestamp", value=str(now))


def log_training_summary(**context):
    """
    Tâche 7: Résumé final du pipeline d'entraînement.
    """
    import logging

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    new_data_stats = ti.xcom_pull(task_ids="check_new_data", key="new_data_stats") or {}
    total_new = ti.xcom_pull(task_ids="check_new_data", key="total_new") or 0
    total_all = ti.xcom_pull(task_ids="check_new_data", key="total_all") or 0
    training_ts = ti.xcom_pull(task_ids="update_training_marker", key="training_timestamp") or "N/A"

    summary = []
    summary.append("=" * 60)
    summary.append("🤖 RÉSUMÉ PIPELINE ML TRAINING")
    summary.append("=" * 60)
    summary.append(f"Date d'exécution: {context.get('execution_date', 'N/A')}")
    summary.append(f"Timestamp entraînement: {training_ts}")
    summary.append("")

    # Nouvelles données
    summary.append("📊 Nouvelles données (depuis dernier training):")
    for symbol, count in new_data_stats.items():
        summary.append(f"  {symbol}: {count} lignes")
    summary.append(f"  Total nouvelles: {total_new}")
    summary.append(f"  Total complètes: {total_all}")
    summary.append("")

    # Modèles entraînés
    summary.append("🧠 Modèles entraînés:")
    summary.append("  • XGBoost — prédiction direction prix")
    summary.append("  • LSTM — prédiction direction prix")
    summary.append("  • Isolation Forest — détection anomalies")
    summary.append("")

    # Services
    summary.append("📡 Résultats disponibles sur:")
    summary.append(f"  • MLflow UI: http://localhost:5001")
    summary.append("")
    summary.append("=" * 60)

    full_summary = "\n".join(summary)
    logger.info(f"\n{full_summary}")

    return full_summary


# ─────────────────────── DAG DEFINITION ──────────────────────────────────

with DAG(
    dag_id="crypto_ml_training",
    default_args=default_args,
    description="Pipeline ML automatique: vérifie nouvelles données → entraîne XGBoost/LSTM/IsolationForest → MLflow",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "ml", "training", "P4", "mlflow"],
) as dag:

    # Tâche 1: Vérification nouvelles données (ShortCircuit)
    t_check = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=check_new_data,
        provide_context=True,
    )

    # Tâche 2: Entraînement prédiction prix (XGBoost + LSTM)
    t_train_price = BashOperator(
        task_id="train_price_predictor",
        bash_command=(
            "docker exec "
            "-e MLFLOW_TRACKING_URI=http://mlflow:5000 "
            "ml-trainer python3 -c \""
            "import sys; sys.path.insert(0, '/app/src'); "
            "from ml.feature_engineering import CryptoFeatureEngineer; "
            "from ml.mlflow_tracking import MLflowExperimentTracker; "
            "fe = CryptoFeatureEngineer(); "
            "tracker = MLflowExperimentTracker(); "
            "symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']; "
            "results = {}; "
            "["
            "  (lambda s: ("
            "    print(f'Training price predictor for {s}...'),"
            "    fe.build_feature_matrix(s, limit=0).__class__.__name__"
            "  ))(s)"
            "  for s in symbols"
            "]; "
            "df = fe.build_feature_matrix('BTCUSDT', limit=0); "
            "fnames = fe.get_feature_names(df); "
            "r = tracker.run_prediction_experiment(df, fnames, symbol='BTCUSDT'); "
            "print(f'Price predictor trained: {r}') "
            "\""
        ),
        execution_timeout=timedelta(hours=1),
    )

    # Tâche 3: Entraînement détection anomalies (Isolation Forest)
    t_train_anomaly = BashOperator(
        task_id="train_anomaly_detector",
        bash_command=(
            "docker exec "
            "-e MLFLOW_TRACKING_URI=http://mlflow:5000 "
            "ml-trainer python3 -c \""
            "import sys; sys.path.insert(0, '/app/src'); "
            "from ml.feature_engineering import CryptoFeatureEngineer; "
            "from ml.mlflow_tracking import MLflowExperimentTracker; "
            "fe = CryptoFeatureEngineer(); "
            "tracker = MLflowExperimentTracker(); "
            "df = fe.build_feature_matrix('BTCUSDT', limit=0); "
            "fnames = fe.get_feature_names(df); "
            "r = tracker.run_anomaly_experiment(df, fnames, symbol='BTCUSDT'); "
            "print(f'Anomaly detector trained: {r}') "
            "\""
        ),
        execution_timeout=timedelta(hours=1),
    )

    # Tâche 4: Détection de drift
    t_drift = BashOperator(
        task_id="run_drift_detection",
        bash_command=(
            "docker exec "
            "-e MLFLOW_TRACKING_URI=http://mlflow:5000 "
            "ml-trainer python3 -c \""
            "import sys; sys.path.insert(0, '/app/src'); "
            "from ml.feature_engineering import CryptoFeatureEngineer; "
            "from ml.drift_detection import DataDriftDetector; "
            "fe = CryptoFeatureEngineer(); "
            "df = fe.build_feature_matrix('BTCUSDT', limit=0); "
            "fnames = fe.get_feature_names(df); "
            "n = len(df); "
            "split = int(n * 0.7); "
            "ref_df = df.iloc[:split]; "
            "curr_df = df.iloc[split:]; "
            "detector = DataDriftDetector(); "
            "report = detector.generate_drift_report(ref_df, curr_df, fnames); "
            "detector.log_drift_to_mlflow(report); "
            "print(f'Drift report: {report[\\\"summary\\\"]}') "
            "\""
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # Tâche 5: Enregistrement des meilleurs modèles dans MLflow Registry
    t_register = BashOperator(
        task_id="register_models",
        bash_command=(
            "docker exec "
            "-e MLFLOW_TRACKING_URI=http://mlflow:5000 "
            "ml-trainer python3 -c \""
            "import sys; sys.path.insert(0, '/app/src'); "
            "from ml.mlflow_tracking import MLflowExperimentTracker; "
            "tracker = MLflowExperimentTracker(); "
            "v1 = tracker.register_best_model('crypto-price-prediction', metric='f1', model_name='crypto-predictor'); "
            "v2 = tracker.register_best_model('crypto-anomaly-detection', metric='f1', model_name='crypto-anomaly-detector'); "
            "print(f'Registered: predictor={v1}, anomaly={v2}'); "
            "if v1: tracker.promote_model('crypto-predictor', v1, 'Production'); "
            "if v2: tracker.promote_model('crypto-anomaly-detector', v2, 'Production'); "
            "print('Models promoted to Production') "
            "\""
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # Tâche 6: Mise à jour marqueur
    t_marker = PythonOperator(
        task_id="update_training_marker",
        python_callable=update_training_marker,
        provide_context=True,
    )

    # Tâche 7: Résumé final
    t_summary = PythonOperator(
        task_id="log_training_summary",
        python_callable=log_training_summary,
        provide_context=True,
    )

    # Flow: check → [price, anomaly] en parallèle → drift → register → marker → summary
    t_check >> [t_train_price, t_train_anomaly]
    [t_train_price, t_train_anomaly] >> t_drift >> t_register >> t_marker >> t_summary
