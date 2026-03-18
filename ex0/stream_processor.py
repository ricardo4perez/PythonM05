from abc import ABC, abstractmethod
from typing import Any, List, Optional


class DataProcessor(ABC):
    """Abstract base class for data processors."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data and return result string."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if data is appropriate for this processor."""
        pass

    def format_output(self, result: str) -> str:
        """Format the output string. Can be overridden by subclasses."""
        return result


class NumericProcessor(DataProcessor):
    """Processor for numeric data (lists of numbers)."""

    def validate(self, data: Any) -> bool:
        """Validate if data is a list of numeric values."""
        try:
            if isinstance(data, list) and len(data) > 0:
                return all(isinstance(x, (int, float)) for x in data)
            return False
        except Exception:
            return False

    def process(self, data: Any) -> str:
        """Process numeric data and calculate sum and average."""
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data")

            total: float = sum(data)
            count: int = len(data)
            average: float = total / count

            result: str = f"Processed {count} numeric values, \
sum={int(total)}, avg={average}"
            return self.format_output(result)
        except Exception as e:
            return f"Error processing numeric data: {str(e)}"


class TextProcessor(DataProcessor):
    """Processor for text data (strings)."""

    def validate(self, data: Any) -> bool:
        """Validate if data is a string."""
        try:
            return isinstance(data, str) and len(data) > 0
        except Exception:
            return False

    def process(self, data: Any) -> str:
        """Process text data and count characters and words."""
        try:
            if not self.validate(data):
                raise ValueError("Invalid text data")

            char_count: int = len(data)
            word_count: int = len(data.split())

            result: str = f"Processed text: {char_count} characters, \
{word_count} words"
            return self.format_output(result)
        except Exception as e:
            return f"Error processing text data: {str(e)}"


class LogProcessor(DataProcessor):
    """Processor for log entries."""

    def validate(self, data: Any) -> bool:
        """Validate if data is a log entry string."""
        try:
            if not isinstance(data, str):
                return False
            log_levels: List[str] = ["ERROR", "WARNING", "INFO", "DEBUG"]
            return any(level in data.upper() for level in log_levels)
        except Exception:
            return False

    def process(self, data: Any) -> str:
        """Process log entry and detect log level."""
        try:
            if not self.validate(data):
                raise ValueError("Invalid log data")

            # Detect log level
            log_levels: List[str] = ["ERROR", "WARNING", "INFO", "DEBUG"]
            detected_level: Optional[str] = None

            for level in log_levels:
                if level in data.upper():
                    detected_level = level
                    break

            if detected_level:
                # Extract message (remove log level prefix if present)
                message: str = data
                if ":" in data:
                    parts: List[str] = data.split(":", 1)
                    if detected_level in parts[0].upper():
                        message = parts[1].strip()

                result: str = f"{detected_level} level detected: {message}"
                return self.format_output(result)

            return "Unknown log format"
        except Exception as e:
            return f"Error processing log data: {str(e)}"

    def format_output(self, result: str) -> str:
        """Format log output with alert prefix for errors."""
        if "ERROR" in result:
            return f"[ALERT] {result}"
        else:
            return f"[INFO] {result}"


def main() -> None:
    """Main function to demonstrate polymorphic data processing."""
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    # Numeric Processor Demo
    print("Initializing Numeric Processor...")
    numeric_processor: NumericProcessor = NumericProcessor()
    numeric_data: List[int] = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")

    if numeric_processor.validate(numeric_data):
        print("Validation: Numeric data verified")

    output: str = numeric_processor.process(numeric_data)
    print(f"Output: {output}")
    print()

    # Text Processor Demo
    print("Initializing Text Processor...")
    text_processor: TextProcessor = TextProcessor()
    text_data: str = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')

    if text_processor.validate(text_data):
        print("Validation: Text data verified")

    output = text_processor.process(text_data)
    print(f"Output: {output}")
    print()

    # Log Processor Demo
    print("Initializing Log Processor...")
    log_processor: LogProcessor = LogProcessor()
    log_data: str = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')

    if log_processor.validate(log_data):
        print("Validation: Log entry verified")

    output = log_processor.process(log_data)
    print(f"Output: {output}")
    print()

    # Polymorphic Processing Demo
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    # Create a list of processors (polymorphic collection)
    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]

    # Different data for each processor
    test_data: List[Any] = [
        [1, 2, 3],
        "Hello World!",
        "INFO: System ready"
    ]

    # Process all data through the same interface
    for i, (processor, data) in enumerate(zip(processors, test_data), 1):
        result: str = processor.process(data)
        print(f"Result {i}: {result}")

    print()
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
