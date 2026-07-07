from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.counter: int = 0
        self.total = 0
        self.list_of_data: list[str] = []
        self.name = ""

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
    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric Processor"

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
                    self.total += 1
                    self.list_of_data.append(str(i))
            else:
                self.total += 1
                self.list_of_data.append(str(data))
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Text Processor"

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
                    self.total += 1
            else:
                self.list_of_data.append(data)
                self.total += 1
        else:
            raise ValueError("Improper Text data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Log Processor"

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
                    self.total += 1
            else:
                self.list_of_data.append(
                        data["log_level"] + ": " + data["log_message"])
                self.total += 1
        else:
            raise ValueError("Improper Log data")


class DataStream():
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            handled = False
            for processor in self.processors:
                if processor.validate(data):
                    handled = True
                    processor.ingest(data)
                    break
            if not handled:
                print(
                        "DataStream error - Can't process "
                        f"element in stream: {data}"
                        )

    def print_processors_stats(self) -> None:
        if not self.processors:
            print("No processor found, no data")
            return
        for processor in self.processors:
            print(
                    f"{processor.name}: total {processor.total}"
                    " items processed, remaining "
                    f"{len(processor.list_of_data)} on processor"
                    )


def main() -> None:
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    data_stream = DataStream()
    stream = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access !Use ssh instead"
                },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
                }
            ],
        42,
        ['Hi', 'five']
        ]
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("\nRegistering Numeric Processor\n")
    data_stream.register_processor(num)
    print("Send first batch of data on stream: ['Hello world', [3.14, -1,"
          "2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access"
          "!Use ssh instead'}, {'log_level': 'INFO', 'log_message': "
          "'User wil is connected'}], 42, ['Hi', 'five']]"
          )
    data_stream.process_stream(stream)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("\nRegistering other data processors")
    data_stream.register_processor(text)
    data_stream.register_processor(log)
    print("Send the same batch again")
    print("== DataStream statistics ==")
    data_stream.process_stream(stream)
    data_stream.print_processors_stats()
    print(
            "\nConsume some elements from the data processors:"
            "Numeric 3, Text 2, Log 1"
          )
    print("== DataStream statistics ==")
    for i in range(3):
        num.output()
    print(f"Numeric Processor: total {num.total} items processed, "
          f"remaining {len(num.list_of_data)} on processor")
    for i in range(2):
        text.output()
    print(f"Text Processor: total {text.total} items processed, "
          f"remaining {len(text.list_of_data)} on processor")
    log.output()
    print(f"Log Processor: total {log.total} items processed, "
          f"remaining {len(log.list_of_data)} on processor")


if __name__ == "__main__":
    main()
