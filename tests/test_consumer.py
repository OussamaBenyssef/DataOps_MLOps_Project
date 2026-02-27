import pytest
from src.ingestion.models import TradeData, KlineData, ValidationError

def test_valid_trade_data():
    raw_data = {
        "e": "trade",
        "E": 123456789,
        "s": "BTCUSDT",
        "p": "50000.5",
        "q": "0.1"
    }
    trade = TradeData.from_dict(raw_data)
    assert trade.event_type == "trade"
    assert trade.symbol == "BTCUSDT"
    assert trade.price == 50000.5
    assert trade.quantity == 0.1

def test_invalid_trade_data():
    raw_data = {
        "e": "trade",
        "s": "BTCUSDT",
        "p": "invalid_price",  # Should be float
        "q": "0.1"
    }
    with pytest.raises(ValidationError):
        TradeData.from_dict(raw_data)

def test_missing_field_trade_data():
    raw_data = {
        "e": "trade",
        "s": "BTCUSDT"
        # missing price and quantity
    }
    with pytest.raises(ValidationError):
        TradeData.from_dict(raw_data)

def test_valid_kline_data():
    raw_data = {
        "e": "kline",
        "E": 123456789,
        "s": "BTCUSDT",
        "k": {
            "t": 123456000,
            "T": 123456999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "50000",
            "c": "50100",
            "h": "50200",
            "l": "49900",
            "v": "10.5",
            "x": False
        }
    }
    kline = KlineData.from_dict(raw_data)
    assert kline.symbol == "BTCUSDT"
    assert kline.kline.open_price == 50000.0
    assert kline.kline.is_closed is False

def test_kline_direct_kwargs():
    # Because of populate_by_name = True, we can pass kwargs directly without aliases
    kline_info = {
        "start_time": 123456000,
        "end_time": 123456999,
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open_price": 50000.0,
        "close_price": 50100.0,
        "high_price": 50200.0,
        "low_price": 49900.0,
        "base_volume": 10.5,
        "is_closed": True
    }
    
    raw_data = {
        "event_type": "kline",
        "event_time": 123456789,
        "symbol": "BTCUSDT",
        "kline": kline_info
    }
    
    kline = KlineData.from_dict(raw_data)
    assert kline.symbol == "BTCUSDT"
    assert kline.kline.open_price == 50000.0
    assert kline.kline.is_closed is True
