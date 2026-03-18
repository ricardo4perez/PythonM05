from abc import ABC, abstractmethod
from typing import List, Any, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class for data streaming functionality"""

    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.processed_count: int = 0
        self.total_batches: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data - must be implemented by subclasses"""
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data based on criteria - default implementation"""
        if criteria is None:
            return data_batch
        return [
            item for item in data_batch
            if criteria.lower() in str(item).lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics - default implementation"""
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "processed_count": self.processed_count,
            "total_batches": self.total_batches
        }


class SensorStream(DataStream):
    """Stream handler for environmental sensor data"""

    _unit = "readings"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")
        self.temperature_sum: float = 0.0
        self.readings: List[Dict[str, float]] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor data batch"""
        try:
            self.total_batches += 1
            temps: List[float] = []

            for item in data_batch:
                if isinstance(item, dict):
                    if "temp" in item:
                        temps.append(float(item["temp"]))
                    self.readings.append(item)
                elif isinstance(item, str) and "temp:" in item:
                    temp_val = float(
                        item.split("temp:")[1].split(",")[0]
                    )
                    temps.append(temp_val)

            self.processed_count += len(data_batch)

            if temps:
                avg_temp = sum(temps) / len(temps)
                self.temperature_sum += sum(temps)
                return (
                    f"Sensor analysis: {len(data_batch)} readings "
                    f"processed, avg temp: {avg_temp:.1f}°C"
                )

            return f"Sensor analysis: {len(data_batch)} readings processed"
        except Exception as e:
            return f"Sensor processing error: {str(e)}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter sensor data with specialized criteria"""
        if criteria == "high-priority" or criteria == "critical":
            # Filter for critical sensor alerts (high temp, low pressure, etc.)
            filtered = []
            for item in data_batch:
                if isinstance(item, dict):
                    if "temp" in item and float(item.get("temp", 0)) > 30:
                        filtered.append(item)
                    elif (
                        "alert" in item or
                        "critical" in str(item).lower()
                    ):
                        filtered.append(item)
                elif isinstance(item, str) and (
                    "alert" in item.lower() or
                    "critical" in item.lower()
                ):
                    filtered.append(item)
            return filtered
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return sensor-specific statistics"""
        stats = super().get_stats()
        if self.processed_count > 0:
            stats["average_temperature"] = (
                self.temperature_sum / self.processed_count
            )
        else:
            stats["average_temperature"] = 0.0
        stats["total_readings"] = len(self.readings)
        return stats


class TransactionStream(DataStream):
    """Stream handler for financial transaction data"""

    _unit = "operations"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self.net_flow: float = 0.0
        self.transactions: List[Dict[str, float]] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process transaction data batch"""
        try:
            self.total_batches += 1
            buy_total: float = 0.0
            sell_total: float = 0.0

            for item in data_batch:
                if isinstance(item, dict):
                    if "buy" in item:
                        buy_total += float(item["buy"])
                    if "sell" in item:
                        sell_total += float(item["sell"])
                    self.transactions.append(item)
                elif isinstance(item, str):
                    if "buy:" in item:
                        buy_val = float(
                            item.split("buy:")[1].split(",")[0]
                        )
                        buy_total += buy_val
                    if "sell:" in item:
                        sell_val = float(
                            item.split("sell:")[1].split(",")[0]
                        )
                        sell_total += sell_val

            self.processed_count += len(data_batch)
            # net_flow: positive = net purchase, negative = net sale
            net = buy_total - sell_total
            self.net_flow += net

            return (
                f"Transaction analysis: {len(data_batch)} operations, "
                f"net flow: {net:+.0f} units"
            )
        except Exception as e:
            return f"Transaction processing error: {str(e)}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter transaction data with specialized criteria"""
        if criteria == "high-priority" or criteria == "large":
            # Filter for large transactions
            filtered = []
            for item in data_batch:
                if isinstance(item, dict):
                    buy_val = float(item.get("buy", 0))
                    sell_val = float(item.get("sell", 0))
                    if buy_val > 100 or sell_val > 100:
                        filtered.append(item)
                elif isinstance(item, str):
                    if "buy:" in item or "sell:" in item:
                        # Extract value and check if large
                        parts = (
                            item.replace("buy:", "")
                            .replace("sell:", "")
                            .split(",")
                        )
                        for part in parts:
                            try:
                                if float(part.strip()) > 100:
                                    filtered.append(item)
                                    break
                            except ValueError:
                                pass
            return filtered
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return transaction-specific statistics"""
        stats = super().get_stats()
        stats["net_flow"] = self.net_flow
        stats["total_transactions"] = len(self.transactions)
        return stats


class EventStream(DataStream):
    """Stream handler for system event data"""

    _unit = "events"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self.error_count: int = 0
        self.events: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process system event batch"""
        try:
            self.total_batches += 1
            errors: int = 0

            for item in data_batch:
                item_str = str(item)
                self.events.append(item_str)
                if "error" in item_str.lower() or "fail" in item_str.lower():
                    errors += 1

            self.processed_count += len(data_batch)
            self.error_count += errors

            if errors > 0:
                if errors == 1:
                    return (
                        f"Event analysis: {len(data_batch)} events, "
                        f"{errors} error detected"
                    )
                else:
                    return (
                        f"Event analysis: {len(data_batch)} events, "
                        f"{errors} errors detected"
                    )

            return f"Event analysis: {len(data_batch)} events processed"
        except Exception as e:
            return f"Event processing error: {str(e)}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter event data with specialized criteria"""
        if criteria == "high-priority" or criteria == "error":
            # Filter for error events
            return [
                item for item in data_batch
                if "error" in str(item).lower() or
                "critical" in str(item).lower()
            ]
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return event-specific statistics"""
        stats = super().get_stats()
        stats["error_count"] = self.error_count
        stats["total_events"] = len(self.events)
        return stats


class StreamProcessor:
    """Handles multiple stream types polymorphically"""

    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Add a stream to the processor"""
        if isinstance(stream, DataStream):
            self.streams.append(stream)

    def process_all(
        self,
        data_batches: Dict[str, List[Any]]
    ) -> List[str]:
        """Process data through all streams polymorphically"""
        results: List[str] = []

        for stream in self.streams:
            stream_name = (
                stream.__class__.__name__.replace("Stream", "").lower()
            )

            if stream_name in data_batches:
                try:
                    batch = data_batches[stream_name]
                    stream.process_batch(batch)
                    class_display = stream.__class__.__name__.replace(
                        'Stream', ''
                    )
                    unit = getattr(stream, '_unit', 'items')
                    results.append(
                        f"- {class_display} data: "
                        f"{len(batch)} {unit} processed"
                    )
                except Exception as e:
                    results.append(
                        f"- {stream.__class__.__name__} error: {str(e)}"
                    )

        return results

    def filter_all(
        self,
        data_batches: Dict[str, List[Any]],
        criteria: str
    ) -> Dict[str, List[Any]]:
        """Filter data across all streams"""
        filtered_results: Dict[str, List[Any]] = {}

        for stream in self.streams:
            stream_name = (
                stream.__class__.__name__.replace("Stream", "").lower()
            )

            if stream_name in data_batches:
                try:
                    filtered = stream.filter_data(
                        data_batches[stream_name], criteria
                    )
                    if filtered:
                        filtered_results[stream_name] = filtered
                except Exception as e:
                    print(
                        f"Filter error in {stream.__class__.__name__}: "
                        f"{str(e)}"
                    )

        return filtered_results


def main() -> None:
    """Demonstrate polymorphic data streaming system"""

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    # Initialize Sensor Stream
    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001")
    print(
        f"Stream ID: {sensor_stream.stream_id}, "
        f"Type: {sensor_stream.stream_type}"
    )

    # Process sensor batch
    sensor_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(
        f"Processing sensor batch: [{sensor_batch[0]}, "
        f"{sensor_batch[1]}, {sensor_batch[2]}]"
    )
    result = sensor_stream.process_batch(sensor_batch)
    print(result)
    print()

    # Initialize Transaction Stream
    print("Initializing Transaction Stream...")
    transaction_stream = TransactionStream("TRANS_001")
    print(
        f"Stream ID: {transaction_stream.stream_id}, "
        f"Type: {transaction_stream.stream_type}"
    )

    # Process transaction batch
    trans_batch = ["buy:100", "sell:150", "buy:75"]
    print(
        f"Processing transaction batch: [{trans_batch[0]}, "
        f"{trans_batch[1]}, {trans_batch[2]}]"
    )
    result = transaction_stream.process_batch(trans_batch)
    print(result)
    print()

    # Initialize Event Stream
    print("Initializing Event Stream...")
    event_stream = EventStream("EVENT_001")
    print(
        f"Stream ID: {event_stream.stream_id}, "
        f"Type: {event_stream.stream_type}"
    )

    # Process event batch
    event_batch = ["login", "error", "logout"]
    print(
        f"Processing event batch: [{event_batch[0]}, "
        f"{event_batch[1]}, {event_batch[2]}]"
    )
    result = event_stream.process_batch(event_batch)
    print(result)
    print()

    # Polymorphic Stream Processing
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    processor = StreamProcessor()
    processor.add_stream(sensor_stream)
    processor.add_stream(transaction_stream)
    processor.add_stream(event_stream)

    # Batch 1
    mixed_data = {
        "sensor": [{"temp": 23.0}, {"temp": 24.5}],
        "transaction": [
            "buy:50", "sell:75", "buy:25", "sell:100"
        ],
        "event": ["startup", "connection", "ready"]
    }

    print("Batch 1 Results:")
    results = processor.process_all(mixed_data)
    for result in results:
        print(result)
    print()

    # Filtering
    print("Stream filtering active: High-priority data only")
    filter_data = {
        "sensor": [
            {"temp": 35.0, "alert": "critical"},
            {"temp": 20.0},
            {"temp": 32.0, "alert": "warning"}
        ],
        "transaction": ["buy:50", "sell:200", "buy:25"],
        "event": ["login", "error", "logout"]
    }

    filtered = processor.filter_all(filter_data, "high-priority")

    sensor_alerts = len(filtered.get("sensor", []))
    transaction_large = len(filtered.get("transaction", []))

    print(
        f"Filtered results: {sensor_alerts} critical sensor alerts, "
        f"{transaction_large} large transaction"
    )
    print()

    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
