import importlib
from app.utils import config


def test_config_defaults(monkeypatch):
    monkeypatch.delenv("INPUT_PATH", raising=False)
    monkeypatch.delenv("OUTPUT_PATH", raising=False)
    monkeypatch.delenv("APP_NAME", raising=False)
    monkeypatch.delenv("WRITE_MODE", raising=False)

    importlib.reload(config)

    assert config.Config.INPUT_PATH == "/opt/spark-apps/data/raw/sales.csv"
    assert config.Config.OUTPUT_PATH == "/opt/spark-apps/data/processed/sales_cleaned.parquet"
    assert config.Config.APP_NAME == "RetailSalesETL"
    assert config.Config.WRITE_MODE == "overwrite"