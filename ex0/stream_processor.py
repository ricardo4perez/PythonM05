
from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    """Abstract base class for all processors"""

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


# ======================
# NUMERIC PROCESSOR
# ======================
class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, list) and all(isinstance(x, (int, float)) for x in data):
            print("Validation: Numeric data verified")
            return True
        raise ValueError("Invalid numeric data")

    def process(self, data: Any) -> str:
        try:
            if self.validate(data):
                count = len(data)
                total = sum(data)
                avg = total / count if count > 0 else 0
                result = f"Processed {count} numeric values, sum={total}, avg={avg}"
                return self.format_output(result)
        except Exception as e:
            return f"Error: {e}"


# ======================
# TEXT PROCESSOR
# ======================
class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            print("Validation: Text data verified")
            return True
        raise ValueError("Invalid text data")

    def process(self, data: Any) -> str:
        try:
            if self.validate(data):
                char_count = len(data)
                word_count = len(data.split())
                result = f"Processed text: {char_count} characters, {word_count} words"
                return self.format_output(result)
        except Exception as e:
            return f"Error: {e}"


# ======================
# LOG PROCESSOR
# ======================
class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str) and ":" in data:
            print("Validation: Log entry verified")
            return True
        raise ValueError("Invalid log format")

    def process(self, data: Any) -> str:
        try:
            if self.validate(data):
                level, message = data.split(":", 1)
                level = level.strip().upper()
                message = message.strip()

                if level == "ERROR":
                    result = f"[ALERT] ERROR level detected: {message}"
                elif level == "INFO":
                    result = f"[INFO] INFO level detected: {message}"
                else:
                    result = f"[LOG] {level}: {message}"

                return self.format_output(result)
        except Exception as e:
            return f"Error: {e}"


# ======================
# MAIN DEMO (POLYMORPHISM)
# ======================
def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    # Individual processors
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("Initializing Numeric Processor...")
    print(numeric.process([1, 2, 3, 4, 5]), "\n")

    print("Initializing Text Processor...")
    print(text.process("Hello Nexus World"), "\n")

    print("Initializing Log Processor...")
    print(log.process("ERROR: Connection timeout"), "\n")

    # Polymorphic demo
    print("=== Polymorphic Processing Demo ===\n")

    processors: List[DataProcessor] = [numeric, text, log]
    data_samples: List[Any] = [
        [1, 2, 3],
        "Hello World",
        "INFO: System ready"
    ]

    print("Processing multiple data types through same interface...\n")

    for i, (processor, data) in enumerate(zip(processors, data_samples), start=1):
        result = processor.process(data)
        print(f"Result {i}: {result}")


if __name__ == "__main__":
    main()
