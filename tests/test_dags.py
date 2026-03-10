"""
Tests Unitaires — DAGs Airflow (P1)
====================================
Tests pour les fonctions tâches des 3 DAGs :
  crypto_daily_pipeline, crypto_backfill, crypto_alertes.
Tous les appels externes (MongoDB, Kafka, Binance API, Airflow) sont mockés.

Note: airflow n'est pas installé en local (uniquement dans Docker),
donc on mocke les modules Airflow avant les imports DAG.

Lancer:
    pytest tests/test_dags.py -v
"""

import pytest
import json
import sys
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, PropertyMock
from types import ModuleType

# ─────────────────────── MOCK AIRFLOW ─────────────────────────────────────
# Airflow n'est pas installé en local, on crée des stubs minimaux

_airflow_mock = ModuleType("airflow")
_airflow_models = ModuleType("airflow.models")
_airflow_models_param = ModuleType("airflow.models.param")
_airflow_operators = ModuleType("airflow.operators")
_airflow_operators_python = ModuleType("airflow.operators.python")
_airflow_operators_bash = ModuleType("airflow.operators.bash")


class FakeDAG:
    """Stub minimal pour airflow.DAG."""
    def __init__(self, dag_id=None, **kwargs):
        self.dag_id = dag_id
        self.tasks = []
        self.schedule_interval = kwargs.get("schedule_interval")
        self._kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeOperator:
    """Stub pour PythonOperator / BashOperator."""
    def __init__(self, task_id=None, **kwargs):
        self.task_id = task_id
        self._kwargs = kwargs
        # Auto-register in active DAG context
        if _active_dag[0]:
            _active_dag[0].tasks.append(self)

    def __rshift__(self, other):
        return other

    def __rrshift__(self, other):
        return self


class FakeParam:
    def __init__(self, **kwargs):
        self._kwargs = kwargs


_active_dag = [None]
_original_enter = FakeDAG.__enter__


def _patched_enter(self):
    _active_dag[0] = self
    return self


def _patched_exit(self, *args):
    _active_dag[0] = None


FakeDAG.__enter__ = _patched_enter
FakeDAG.__exit__ = _patched_exit

# Handle >> for lists (Airflow uses [t1, t2] >> t3)
_orig_list_class = list


class TaskList(list):
    def __rshift__(self, other):
        return other

    def __rrshift__(self, other):
        return self


# Monkey-patch the builtins for DAG imports
import builtins
_orig_build_class = builtins.__build_class__

# Install mocks
_airflow_mock.DAG = FakeDAG
_airflow_models_param.Param = FakeParam
_airflow_operators_python.PythonOperator = FakeOperator
_airflow_operators_python.BranchPythonOperator = FakeOperator
_airflow_operators_python.ShortCircuitOperator = FakeOperator
_airflow_operators_bash.BashOperator = FakeOperator
_airflow_operators.python = _airflow_operators_python
_airflow_operators.bash = _airflow_operators_bash
_airflow_models.param = _airflow_models_param

sys.modules["airflow"] = _airflow_mock
sys.modules["airflow.models"] = _airflow_models
sys.modules["airflow.models.param"] = _airflow_models_param
sys.modules["airflow.operators"] = _airflow_operators
sys.modules["airflow.operators.python"] = _airflow_operators_python
sys.modules["airflow.operators.bash"] = _airflow_operators_bash

# Ajouter les dossiers au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))


# ─────────────────────── HELPERS ──────────────────────────────────────────

def make_mock_context(params=None):
    """Crée un contexte Airflow simulé avec XCom."""
    xcom_store = {}

    class MockTI:
        def xcom_push(self, key, value):
            xcom_store[key] = value

        def xcom_pull(self, task_ids=None, key=None):
            if key:
                return xcom_store.get(key)
            return xcom_store

    ctx = {
        "ti": MockTI(),
        "params": params or {},
        "execution_date": datetime(2024, 2, 15),
    }
    ctx["_xcom_store"] = xcom_store
    return ctx


def make_mock_ohlcv_doc(symbol="BTCUSDT", ts_offset_minutes=0, close=45000.0):
    """Crée un document OHLCV simulé MongoDB."""
    ts = datetime.now(timezone.utc) - timedelta(minutes=ts_offset_minutes)
    return {
        "symbol": symbol,
        "interval": "1m",
        "timestamp": ts,
        "open": close - 50,
        "high": close + 100,
        "low": close - 100,
        "close": close,
        "volume": 1234.56,
    }


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: DAG Loading — les DAGs se chargent sans erreur
# ═══════════════════════════════════════════════════════════════════════════

class TestDAGLoading:
    """Vérifie que les DAGs sont syntaxiquement corrects et se chargent."""

    def test_backfill_dag_loads(self):
        """Le DAG crypto_backfill se charge et a les bons task_ids."""
        from dags.crypto_backfill import dag_backfill
        assert dag_backfill.dag_id == "crypto_backfill"
        assert dag_backfill.schedule_interval is None
        task_ids = [t.task_id for t in dag_backfill.tasks]
        assert "detect_gaps" in task_ids
        assert "backfill_from_binance" in task_ids
        assert "trigger_spark_etl" in task_ids
        assert "validate_backfill" in task_ids
        assert "log_backfill_report" in task_ids

    def test_alertes_dag_loads(self):
        """Le DAG crypto_alertes se charge et a les bons task_ids."""
        from dags.crypto_alertes import dag_alertes
        assert dag_alertes.dag_id == "crypto_alertes"
        assert dag_alertes.schedule_interval == "@hourly"
        task_ids = [t.task_id for t in dag_alertes.tasks]
        assert "check_data_freshness" in task_ids
        assert "detect_market_anomalies" in task_ids
        assert "check_pipeline_health" in task_ids
        assert "decide_alert_level" in task_ids

    def test_daily_pipeline_dag_loads(self):
        """Le DAG crypto_daily_pipeline se charge et a les bons task_ids."""
        from dags.crypto_daily_pipeline import dag
        assert dag.dag_id == "crypto_daily_pipeline"
        assert dag.schedule_interval == "@daily"
        task_ids = [t.task_id for t in dag.tasks]
        # Pipeline principal (5 tâches)
        assert "check_services" in task_ids
        assert "collect_historical_data" in task_ids
        assert "trigger_spark_etl" in task_ids
        assert "compute_quality_metrics" in task_ids
        assert "log_pipeline_summary" in task_ids
        # DataHub ingestion + lineage (3 tâches)
        assert "ingest_datahub_kafka" in task_ids
        assert "ingest_datahub_mongodb" in task_ids
        assert "emit_datahub_lineage" in task_ids

    def test_daily_pipeline_has_8_tasks(self):
        """Le DAG crypto_daily_pipeline contient exactement 8 tâches."""
        from dags.crypto_daily_pipeline import dag
        assert len(dag.tasks) == 8, (
            f"Attendu 8 tâches, trouvé {len(dag.tasks)}: "
            f"{[t.task_id for t in dag.tasks]}"
        )

    def test_daily_pipeline_datahub_tags(self):
        """Le DAG a le tag 'datahub'."""
        from dags.crypto_daily_pipeline import dag
        assert "datahub" in dag._kwargs.get("tags", [])


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Backfill DAG — task functions
# ═══════════════════════════════════════════════════════════════════════════

class TestBackfillDAG:
    """Tests des fonctions tâches du DAG crypto_backfill."""

    @patch("pymongo.MongoClient")
    def test_detect_gaps_finds_missing_data(self, mock_mongo_cls):
        """detect_gaps calcule correctement les gaps de couverture."""
        from dags.crypto_backfill import detect_gaps

        mock_coll = MagicMock()
        mock_coll.count_documents.return_value = 100
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_coll)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context({
            "start_date": "2024-02-01",
            "end_date": "2024-02-02",
            "symbols": "BTCUSDT",
            "interval": "1m",
        })

        result = detect_gaps(**ctx)

        assert "BTCUSDT" in result
        assert result["BTCUSDT"]["existing"] == 100
        assert result["BTCUSDT"]["expected"] > 0
        assert result["BTCUSDT"]["missing"] >= 0
        assert 0 <= result["BTCUSDT"]["coverage_pct"] <= 100
        assert ctx["_xcom_store"]["gaps_report"] == result
        assert "backfill_params" in ctx["_xcom_store"]

    @patch("pymongo.MongoClient")
    def test_detect_gaps_empty_collection(self, mock_mongo_cls):
        """detect_gaps avec collection vide → coverage 0%."""
        from dags.crypto_backfill import detect_gaps

        mock_coll = MagicMock()
        mock_coll.count_documents.return_value = 0
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_coll)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context({
            "start_date": "2024-02-01",
            "end_date": "2024-02-02",
            "symbols": "BTCUSDT",
            "interval": "1m",
        })

        result = detect_gaps(**ctx)
        assert result["BTCUSDT"]["existing"] == 0
        assert result["BTCUSDT"]["coverage_pct"] == 0

    @patch("time.sleep")
    @patch("kafka.KafkaProducer")
    @patch("requests.get")
    def test_backfill_publishes_flat_format(self, mock_requests_get, mock_kafka_cls, _mock_sleep):
        """backfill_from_binance publie au format plat (pas WebSocket)."""
        from dags.crypto_backfill import backfill_from_binance

        mock_response_data = MagicMock()
        mock_response_data.json.return_value = [
            [1706745600000, "42000.0", "42100.0", "41900.0", "42050.0",
             "10.5", 1706745659999, "441525.0", 150, "5.2", "218520.0", "0"],
        ]
        mock_response_data.raise_for_status = MagicMock()
        mock_response_empty = MagicMock()
        mock_response_empty.json.return_value = []
        mock_response_empty.raise_for_status = MagicMock()
        # Premier appel retourne des klines, le second retourne vide → sortie de boucle
        mock_requests_get.side_effect = [mock_response_data, mock_response_empty]

        mock_producer = MagicMock()
        mock_kafka_cls.return_value = mock_producer

        ctx = make_mock_context()
        ctx["_xcom_store"]["backfill_params"] = {
            "start_date": "2024-02-01",
            "end_date": "2024-02-01",
            "symbols": ["BTCUSDT"],
            "interval": "1m",
        }

        result = backfill_from_binance(**ctx)

        assert mock_producer.send.called
        call_args = mock_producer.send.call_args_list[0]
        assert call_args[0][0] == "raw_klines"

        msg = call_args[1]["value"]
        # Format plat vérifié
        assert "symbol" in msg
        assert "open" in msg
        assert "close" in msg
        assert "volume" in msg
        assert "trades_count" in msg
        assert "stream" not in msg  # Pas d'enveloppe WebSocket
        assert "data" not in msg
        assert isinstance(msg["open"], float)
        assert isinstance(msg["trades_count"], int)

    def test_validate_backfill_logic(self):
        """validate_backfill compare avant/après correctement."""
        from dags.crypto_backfill import validate_backfill

        ctx = make_mock_context()
        ctx["_xcom_store"]["gaps_report"] = {
            "BTCUSDT": {"expected": 1440, "existing": 1000, "missing": 440, "coverage_pct": 69.4},
        }
        ctx["_xcom_store"]["backfill_params"] = {"symbols": ["BTCUSDT"]}
        ctx["_xcom_store"]["backfill_stats"] = {"BTCUSDT": 440}

        result = validate_backfill(**ctx)

        assert "BTCUSDT" in result
        assert result["BTCUSDT"]["avant_coverage"] == 69.4
        assert result["BTCUSDT"]["klines_collectées"] == 440
        assert result["BTCUSDT"]["manquantes_avant"] == 440

    def test_log_backfill_report_format(self):
        """log_backfill_report génère un rapport avec les infos clés."""
        from dags.crypto_backfill import log_backfill_report

        ctx = make_mock_context()
        ctx["_xcom_store"]["gaps_report"] = {
            "BTCUSDT": {"expected": 1440, "existing": 1000, "missing": 440, "coverage_pct": 69.4},
        }
        ctx["_xcom_store"]["backfill_stats"] = {"BTCUSDT": 440}
        ctx["_xcom_store"]["backfill_total"] = 440
        ctx["_xcom_store"]["validation"] = {}
        ctx["_xcom_store"]["backfill_params"] = {
            "start_date": "2024-02-01", "end_date": "2024-02-02", "interval": "1m",
        }

        report = log_backfill_report(**ctx)

        assert "RAPPORT BACKFILL" in report
        assert "2024-02-01" in report
        assert "BTCUSDT" in report
        assert "440" in report


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: Alertes DAG — task functions
# ═══════════════════════════════════════════════════════════════════════════

class TestAlertesDAG:
    """Tests des fonctions tâches du DAG crypto_alertes."""

    @patch("pymongo.MongoClient")
    def test_freshness_fresh_data(self, mock_mongo_cls):
        """Données fraîches (< 10 min) → 0 alertes freshness."""
        from dags.crypto_alertes import check_data_freshness

        fresh_doc = make_mock_ohlcv_doc("BTCUSDT", ts_offset_minutes=2)
        mock_coll = MagicMock()
        mock_coll.find_one.return_value = fresh_doc
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_coll)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        check_data_freshness(**ctx)

        alerts = ctx["_xcom_store"]["freshness_alerts"]
        assert len(alerts) == 0
        for info in ctx["_xcom_store"]["freshness"].values():
            assert info["status"] == "FRESH"

    @patch("pymongo.MongoClient")
    def test_freshness_stale_data(self, mock_mongo_cls):
        """Données vieilles (> 10 min) → alertes data_stale."""
        from dags.crypto_alertes import check_data_freshness

        stale_doc = make_mock_ohlcv_doc("BTCUSDT", ts_offset_minutes=60)
        mock_coll = MagicMock()
        mock_coll.find_one.return_value = stale_doc
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_coll)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        check_data_freshness(**ctx)

        alerts = ctx["_xcom_store"]["freshness_alerts"]
        assert len(alerts) >= 1
        assert all(a["type"] == "data_stale" for a in alerts)

    @patch("pymongo.MongoClient")
    def test_freshness_missing_data(self, mock_mongo_cls):
        """Pas de données → alertes data_missing."""
        from dags.crypto_alertes import check_data_freshness

        mock_coll = MagicMock()
        mock_coll.find_one.return_value = None
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_coll)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        check_data_freshness(**ctx)

        alerts = ctx["_xcom_store"]["freshness_alerts"]
        assert len(alerts) >= 1
        assert any(a["type"] == "data_missing" for a in alerts)

    @patch("pymongo.MongoClient")
    def test_market_anomalies_price_spike(self, mock_mongo_cls):
        """Price spike > 5% → alerte price_spike."""
        from dags.crypto_alertes import detect_market_anomalies

        candles = [
            make_mock_ohlcv_doc("BTCUSDT", ts_offset_minutes=0, close=49500.0),
            make_mock_ohlcv_doc("BTCUSDT", ts_offset_minutes=1, close=45000.0),
        ]

        mock_ohlcv = MagicMock()
        mock_ohlcv.find.return_value = MagicMock(limit=MagicMock(return_value=candles))
        mock_indicators = MagicMock()
        mock_indicators.find_one.return_value = None
        mock_anomalies = MagicMock()

        def getitem(name):
            return {"ohlcv": mock_ohlcv, "indicators": mock_indicators, "anomalies": mock_anomalies}.get(name, MagicMock())

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        detect_market_anomalies(**ctx)

        alerts = ctx["_xcom_store"]["market_alerts"]
        price_spikes = [a for a in alerts if a["type"] == "price_spike"]
        assert len(price_spikes) >= 1

    @patch("pymongo.MongoClient")
    def test_market_anomalies_rsi_extreme(self, mock_mongo_cls):
        """RSI < 20 → alerte rsi_extreme."""
        from dags.crypto_alertes import detect_market_anomalies

        mock_ohlcv = MagicMock()
        mock_ohlcv.find.return_value = MagicMock(limit=MagicMock(return_value=[]))
        mock_indicators = MagicMock()
        mock_indicators.find_one.return_value = {"rsi_14": 15.0, "symbol": "BTCUSDT"}
        mock_anomalies = MagicMock()

        def getitem(name):
            return {"ohlcv": mock_ohlcv, "indicators": mock_indicators, "anomalies": mock_anomalies}.get(name, MagicMock())

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        detect_market_anomalies(**ctx)

        rsi_alerts = [a for a in ctx["_xcom_store"]["market_alerts"] if a["type"] == "rsi_extreme"]
        assert len(rsi_alerts) >= 1

    @patch("pymongo.MongoClient")
    def test_pipeline_health_high_null_rate(self, mock_mongo_cls):
        """Taux de nulls élevé → alerte null_rate_high."""
        from dags.crypto_alertes import check_pipeline_health

        docs_with_nulls = [
            {"symbol": "BTCUSDT", "open": None, "high": 42100.0, "low": 41900.0, "close": 42050.0, "volume": 10.5},
            {"symbol": "BTCUSDT", "open": None, "high": None, "low": None, "close": None, "volume": None},
        ]

        mock_ohlcv = MagicMock()
        mock_ohlcv.find.return_value = MagicMock(limit=MagicMock(return_value=docs_with_nulls))
        mock_ohlcv.count_documents.return_value = 2
        mock_ohlcv.aggregate.return_value = []
        mock_indicators = MagicMock()
        mock_indicators.count_documents.return_value = 2

        def getitem(name):
            return {"ohlcv": mock_ohlcv, "indicators": mock_indicators}.get(name, MagicMock())

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        check_pipeline_health(**ctx)

        null_alerts = [a for a in ctx["_xcom_store"]["health_alerts"] if a["type"] == "null_rate_high"]
        assert len(null_alerts) >= 1

    def test_decide_alert_level_critical(self):
        """Alertes critiques → branche 'generate_critical_report'."""
        from dags.crypto_alertes import decide_alert_level

        ctx = make_mock_context()
        ctx["_xcom_store"]["freshness_alerts"] = [
            {"type": "data_missing", "severity": "critical", "symbol": "BTCUSDT", "message": "test"}
        ]
        ctx["_xcom_store"]["market_alerts"] = []
        ctx["_xcom_store"]["health_alerts"] = []

        assert decide_alert_level(**ctx) == "generate_critical_report"

    def test_decide_alert_level_normal(self):
        """Pas d'alertes critiques → branche 'generate_normal_report'."""
        from dags.crypto_alertes import decide_alert_level

        ctx = make_mock_context()
        ctx["_xcom_store"]["freshness_alerts"] = [
            {"type": "data_stale", "severity": "medium", "symbol": "BTCUSDT", "message": "test"}
        ]
        ctx["_xcom_store"]["market_alerts"] = []
        ctx["_xcom_store"]["health_alerts"] = []

        assert decide_alert_level(**ctx) == "generate_normal_report"

    def test_critical_report_format(self):
        """Le rapport critique contient les alertes classées par sévérité."""
        from dags.crypto_alertes import generate_critical_report

        ctx = make_mock_context()
        ctx["_xcom_store"]["freshness_alerts"] = [
            {"type": "data_missing", "severity": "critical", "symbol": "BTCUSDT", "message": "No data"}
        ]
        ctx["_xcom_store"]["market_alerts"] = [
            {"type": "price_spike", "severity": "high", "symbol": "ETHUSDT", "message": "Spike 8%"}
        ]
        ctx["_xcom_store"]["health_alerts"] = []

        report = generate_critical_report(**ctx)
        assert "CRITIQUE" in report
        assert "No data" in report
        assert "Spike 8%" in report

    def test_normal_report_format(self):
        """Le rapport normal contient la fraîcheur et 'Aucune alerte'."""
        from dags.crypto_alertes import generate_normal_report

        ctx = make_mock_context()
        ctx["_xcom_store"]["freshness"] = {"BTCUSDT": {"status": "FRESH", "age_minutes": 2.5}}
        ctx["_xcom_store"]["freshness_alerts"] = []
        ctx["_xcom_store"]["market_alerts"] = []
        ctx["_xcom_store"]["health_alerts"] = []

        report = generate_normal_report(**ctx)
        assert "BTCUSDT" in report
        assert "FRESH" in report
        assert "Aucune alerte" in report


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Daily Pipeline DAG — task functions
# ═══════════════════════════════════════════════════════════════════════════

class TestDailyPipelineDAG:
    """Tests des fonctions tâches du DAG crypto_daily_pipeline."""

    @patch("socket.create_connection", side_effect=OSError("Connection refused"))
    def test_check_services_raises_when_down(self, _mock_socket):
        """check_services lève RuntimeError si services inaccessibles."""
        from dags.crypto_daily_pipeline import check_services

        ctx = make_mock_context()
        with pytest.raises(RuntimeError, match="Services indisponibles"):
            check_services(**ctx)

    @patch("time.sleep")
    @patch("kafka.KafkaProducer")
    @patch("requests.get")
    def test_collect_data_flat_format(self, mock_requests_get, mock_kafka_cls, _mock_sleep):
        """collect_historical_data publie au format plat."""
        from dags.crypto_daily_pipeline import collect_historical_data

        mock_response_data = MagicMock()
        mock_response_data.json.return_value = [
            [1706745600000, "42000.0", "42100.0", "41900.0", "42050.0",
             "10.5", 1706745659999, "441525.0", 150, "5.2", "218520.0", "0"],
        ]
        mock_response_data.raise_for_status = MagicMock()
        mock_response_empty = MagicMock()
        mock_response_empty.json.return_value = []
        mock_response_empty.raise_for_status = MagicMock()
        # Premier appel retourne des klines, les suivants retournent vide
        mock_requests_get.side_effect = [mock_response_data, mock_response_empty] * 3  # ×3 paires

        mock_producer = MagicMock()
        mock_kafka_cls.return_value = mock_producer

        ctx = make_mock_context()
        collect_historical_data(**ctx)

        assert mock_producer.send.called
        msg = mock_producer.send.call_args_list[0][1]["value"]
        assert "symbol" in msg
        assert "open" in msg
        assert "stream" not in msg
        assert "data" not in msg
        assert isinstance(msg["open"], float)

    @patch("pymongo.MongoClient")
    def test_compute_quality_metrics(self, mock_mongo_cls):
        """compute_quality_metrics compte les documents."""
        from dags.crypto_daily_pipeline import compute_quality_metrics

        mock_coll = MagicMock()
        mock_coll.count_documents.return_value = 42
        mock_coll.find.return_value = MagicMock(limit=MagicMock(return_value=[]))
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_coll)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        result = compute_quality_metrics(**ctx)

        assert "ohlcv_total" in result
        assert result["ohlcv_total"] == 42

    def test_log_pipeline_summary_format(self):
        """log_pipeline_summary génère un résumé lisible."""
        from dags.crypto_daily_pipeline import log_pipeline_summary

        ctx = make_mock_context()
        ctx["_xcom_store"]["services_status"] = {"Kafka": "✅ UP", "MongoDB": "✅ UP"}
        ctx["_xcom_store"]["collection_stats"] = {"BTCUSDT": 1440, "ETHUSDT": 1440}
        ctx["_xcom_store"]["total_klines"] = 2880
        ctx["_xcom_store"]["quality_metrics"] = {"ohlcv_total": 5000}

        report = log_pipeline_summary(**ctx)

        assert "RÉSUMÉ PIPELINE" in report
        assert "BTCUSDT" in report
        assert "2880" in report


# ═══════════════════════════════════════════════════════════════════════════
# TEST 5: ML Training DAG — task functions
# ═══════════════════════════════════════════════════════════════════════════

class TestMLTrainingDAG:
    """Tests des fonctions tâches du DAG crypto_ml_training."""

    def test_ml_training_dag_loads(self):
        """Le DAG crypto_ml_training se charge et a les bons task_ids."""
        from dags.crypto_ml_training import dag
        assert dag.dag_id == "crypto_ml_training"
        assert dag.schedule_interval == "@daily"
        task_ids = [t.task_id for t in dag.tasks]
        assert "check_new_data" in task_ids
        assert "train_price_predictor" in task_ids
        assert "train_anomaly_detector" in task_ids
        assert "run_drift_detection" in task_ids
        assert "register_models" in task_ids
        assert "run_batch_anomaly_inference" in task_ids   # NOUVEAU
        assert "save_batch_predictions" in task_ids        # NOUVEAU
        assert "update_training_marker" in task_ids
        assert "log_training_summary" in task_ids
        assert len(dag.tasks) == 9, (
            f"Attendu 9 tâches, trouvé {len(dag.tasks)}: "
            f"{[t.task_id for t in dag.tasks]}"
        )

    @patch("pymongo.MongoClient")
    def test_check_new_data_enough(self, mock_mongo_cls):
        """check_new_data retourne True si assez de nouvelles données."""
        from dags.crypto_ml_training import check_new_data

        # Mock ml_metadata — dernier training il y a 1 jour
        mock_metadata = {
            "_id": "last_training",
            "timestamp": datetime(2024, 2, 14, tzinfo=timezone.utc),
        }
        mock_ohlcv = MagicMock()
        mock_ohlcv.count_documents.return_value = 500  # Par symbole
        mock_ml_meta = MagicMock()
        mock_ml_meta.find_one.return_value = mock_metadata

        def getitem(name):
            if name == "ml_metadata":
                return mock_ml_meta
            return mock_ohlcv

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        result = check_new_data(**ctx)

        assert result is True
        assert ctx["_xcom_store"]["total_new"] == 1500  # 500 × 3 paires

    @patch("pymongo.MongoClient")
    def test_check_new_data_not_enough(self, mock_mongo_cls):
        """check_new_data retourne False si pas assez de nouvelles données."""
        from dags.crypto_ml_training import check_new_data

        mock_metadata = {
            "_id": "last_training",
            "timestamp": datetime(2024, 2, 14, tzinfo=timezone.utc),
        }
        mock_ohlcv = MagicMock()
        mock_ohlcv.count_documents.return_value = 50  # Trop peu
        mock_ml_meta = MagicMock()
        mock_ml_meta.find_one.return_value = mock_metadata

        def getitem(name):
            if name == "ml_metadata":
                return mock_ml_meta
            return mock_ohlcv

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        result = check_new_data(**ctx)

        assert result is False
        assert ctx["_xcom_store"]["total_new"] == 150  # 50 × 3

    @patch("pymongo.MongoClient")
    def test_check_new_data_first_run(self, mock_mongo_cls):
        """Premier run (pas de marqueur) → tout est considéré comme nouveau."""
        from dags.crypto_ml_training import check_new_data

        mock_ohlcv = MagicMock()
        mock_ohlcv.count_documents.return_value = 2000
        mock_ml_meta = MagicMock()
        mock_ml_meta.find_one.return_value = None  # Pas de marqueur

        def getitem(name):
            if name == "ml_metadata":
                return mock_ml_meta
            return mock_ohlcv

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        result = check_new_data(**ctx)

        assert result is True

    @patch("pymongo.MongoClient")
    def test_update_training_marker(self, mock_mongo_cls):
        """update_training_marker écrit dans ml_metadata."""
        from dags.crypto_ml_training import update_training_marker

        mock_ml_meta = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_ml_meta)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_mongo_cls.return_value = mock_client

        ctx = make_mock_context()
        ctx["_xcom_store"]["total_new"] = 1500
        ctx["_xcom_store"]["total_all"] = 10000

        update_training_marker(**ctx)

        mock_ml_meta.update_one.assert_called_once()
        assert "training_timestamp" in ctx["_xcom_store"]

    def test_log_training_summary_format(self):
        """log_training_summary génère un résumé lisible."""
        from dags.crypto_ml_training import log_training_summary

        ctx = make_mock_context()
        ctx["_xcom_store"]["new_data_stats"] = {"BTCUSDT": 500, "ETHUSDT": 400, "BNBUSDT": 300}
        ctx["_xcom_store"]["total_new"] = 1200
        ctx["_xcom_store"]["total_all"] = 10000
        ctx["_xcom_store"]["training_timestamp"] = "2024-02-15T12:00:00"

        report = log_training_summary(**ctx)

        assert "RÉSUMÉ PIPELINE ML TRAINING" in report
        assert "BTCUSDT" in report
        assert "1200" in report
        assert "XGBoost" in report


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
