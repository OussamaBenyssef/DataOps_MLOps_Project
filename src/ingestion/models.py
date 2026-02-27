from dataclasses import dataclass

class ValidationError(Exception):
    pass

@dataclass
class TradeData:
    """
    Standard dataclass model for validating real-time trade data.
    Aliases map to the standard Binance WebSocket stream fields.
    """
    event_type: str
    event_time: int
    symbol: str
    price: float
    quantity: float

    @classmethod
    def from_dict(cls, data: dict) -> 'TradeData':
        try:
            # e = event type, E = event time, s = symbol, p = price, q = quantity
            return cls(
                event_type=data.get("e", "trade") if "e" in data else data.get("event_type", "trade"),
                event_time=int(data.get("E", data.get("event_time", 0))),
                symbol=str(data.get("s", data.get("symbol"))),
                price=float(data.get("p", data.get("price"))),
                quantity=float(data.get("q", data.get("quantity")))
            )
        except (ValueError, TypeError, KeyError) as e:
            raise ValidationError(f"Trade validation failed: {str(e)}")

@dataclass
class KlineInfo:
    """
    Standard dataclass model for the inner candlestick data from Binance WebSocket.
    """
    start_time: int
    end_time: int
    symbol: str
    interval: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    base_volume: float
    is_closed: bool

    @classmethod
    def from_dict(cls, data: dict) -> 'KlineInfo':
        try:
            return cls(
                start_time=int(data.get("t", data.get("start_time"))),
                end_time=int(data.get("T", data.get("end_time"))),
                symbol=str(data.get("s", data.get("symbol"))),
                interval=str(data.get("i", data.get("interval"))),
                open_price=float(data.get("o", data.get("open_price"))),
                close_price=float(data.get("c", data.get("close_price"))),
                high_price=float(data.get("h", data.get("high_price"))),
                low_price=float(data.get("l", data.get("low_price"))),
                base_volume=float(data.get("v", data.get("base_volume"))),
                is_closed=bool(data.get("x", data.get("is_closed")))
            )
        except (ValueError, TypeError, KeyError) as e:
            raise ValidationError(f"Kline info validation failed: {str(e)}")

@dataclass
class KlineData:
    """
    Standard dataclass model for the outer Kline/Candlestick event from Binance WebSocket.
    """
    event_type: str
    event_time: int
    symbol: str
    kline: KlineInfo

    @classmethod
    def from_dict(cls, data: dict) -> 'KlineData':
        try:
            return cls(
                event_type=data.get("e", "kline") if "e" in data else data.get("event_type", "kline"),
                event_time=int(data.get("E", data.get("event_time", 0))),
                symbol=str(data.get("s", data.get("symbol"))),
                kline=KlineInfo.from_dict(data.get("k", data.get("kline", {})))
            )
        except (ValueError, TypeError, KeyError) as e:
            raise ValidationError(f"Kline validation failed: {str(e)}")
    
