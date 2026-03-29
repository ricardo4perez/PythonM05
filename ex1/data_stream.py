from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


# ======================
# BASE CLASS
# ======================
class DataStream(ABC):

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        return [d for d in data_batch if criteria.lower() in str(d).lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "processed_count": self.processed_count
        }


# ======================
# SENSOR STREAM
# ======================
class SensorStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            temps = [d.get("temp", 0) for d in data_batch if isinstance(d, dict)]
            avg_temp = sum(temps) / len(temps) if temps else 0

            return f"Sensor data: {len(data_batch)} readings processed, avg temp: {avg_temp:.1f}°C"
        except Exception as e:
            return f"Sensor error: {e}"


# ======================
# TRANSACTION STREAM
# ======================
class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            balance = 0
            for t in data_batch:
                if isinstance(t, dict):
                    if t.get("type") == "buy":
                        balance -= t.get("amount", 0)
                    elif t.get("type") == "sell":
                        balance += t.get("amount", 0)

            return f"Transaction data: {len(data_batch)} operations processed, net flow: {balance}"
        except Exception as e:
            return f"Transaction error: {e}"


# ======================
# EVENT STREAM
# ======================
class EventStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            errors = [e for e in data_batch if "error" in str(e).lower()]
            return f"Event data: {len(data_batch)} events processed, {len(errors)} errors detected"
        except Exception as e:
            return f"Event error: {e}"


# ======================
# STREAM PROCESSOR (POLYMORPHISM)
# ======================
class StreamProcessor:

    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(self, batches: List[List[Any]]) -> None:
        print("Processing mixed stream types through unified interface...\n")

        for stream, batch in zip(self.streams, batches):
            result = stream.process_batch(batch)
            print(result)


# ======================
# MAIN DEMO
# ======================
def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    batches = [
        [{"temp": 22.5}, {"temp": 23.0}],
        [{"type": "buy", "amount": 100}, {"type": "sell", "amount": 150}],
        ["login", "error", "logout"]
    ]

    processor.process_all(batches)

    print("\nStats:")
    for s in processor.streams:
        print(s.get_stats())


if __name__ == "__main__":
    main()
