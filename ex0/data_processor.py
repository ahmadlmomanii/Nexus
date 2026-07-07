from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.counter: int = -1
        self.list_of_data: list[str] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.list_of_data:
            raise IndexError("No Data Provided")
        self.counter += 1
        out: str = self.list_of_data.pop(0)
        return self.counter, out


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for i in data:
                if not isinstance(i, int | float):
                    return False
            return True
        if isinstance(data, int | float):
            return True
        return False

    def ingest(self, data: float | int | list[int | float]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for i in data:
                    self.list_of_data.append(str(i))
            else:
                self.list_of_data.append(str(data))
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for i in data:
                if not isinstance(i, str):
                    return False
            return True
        if isinstance(data, str):
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for i in data:
                    self.list_of_data.append(i)
            else:
                self.list_of_data.append(data)
        else:
            raise ValueError("Improper Text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for i in data:
                if not isinstance(i, dict):
                    return False
                for key, value in i.items():
                    if (
                            not isinstance(key, str)
                            or not isinstance(value, str)
                            ):
                        return False
            return True
        if isinstance(data, dict):
            for key, value in data.items():
                if (
                        not isinstance(key, str)
                        or not isinstance(value, str)
                        ):
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for i in data:
                    self.list_of_data.append(
                            i["log_level"] + ": " + i["log_message"])
            else:
                self.list_of_data.append(
                        data["log_level"] + ": " + data["log_message"])
        else:
            raise ValueError("Improper Log data")


def main() -> None:
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    try:
        print("=== Code Nexus - Data Processor ===")
        print("Testing Numeric Processor...")
        print(f"Trying to validate input '42': {num.validate(42)}")
        print(f"Trying to validate input 'Hello': {num.validate("Hello")}")
        print("Test invalid ingestion of string",
              "'foo' without prior validation:")
        try:
            num.ingest("foo")
        except ValueError as e:
            print(f"Got exception: {e}")
        print("Processing data: [1, 2, 3, 4, 5]")
        num.ingest([1, 2, 3, 4, 5])
        print("Extracting 3 values...")
        for i in range(3):
            rank, data = num.output()
            print(f"Numeric value {rank}: {data}")
        print("\nTesting Text Processor...")
        print(f"Trying to validate input ’42’: {text.validate(42)}")
        print("Processing data: [’Hello’, ’Nexus’, ’World’]")
        print("Extracting 1 value...")
        text.ingest(["Hello", "Nexus", "World"])
        rank, data = text.output()
        print(f"Text value {rank}: {data}")
        print("Testing Log Processor...")
        print(f"Trying to validate input ’Hello’: {log.validate("Hello")}")
        print("Processing data: [{’log_level’: ’NOTICE’, ’log_message’:",
              "’Connection to server’}, {’log_level’: ’ERROR’,",
              "’log_message’: ’Unauthorized access!!’}]")
        log.ingest([
            {"log_level": "NOTICE", "log_message": "Connection to server"},
            {"log_level": "ERROR", "log_message": "Unauthorized access!!"}
            ])
        print("Extracting 2 values...")
        rank, data = log.output()
        print(f"Log entry {rank}: {data}")
        rank, data = log.output()
        print(f"Log entry {rank}: {data}")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
