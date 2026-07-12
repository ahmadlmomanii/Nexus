from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.counter: int = -1
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            ls = []
            for i in range(nb):
                try:
                    res = proc.output()
                    ls.append(res)
                except IndexError:
                    break
            plugin.process_output(ls)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        values = [value for rank, value in data]
        print(*values, sep=",")


class JSONPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ls = []
        print("JSON Output:")
        for rank, value in data:
            ls.append(f'"item_{rank}": "{value}"')
        print("{", end="")
        print(*ls, sep=", ", end="")
        print("}")


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
    print("=== Code Nexus - Data pipline ===\n")
    print("Initialize Data Stream...\n")
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("\nRegistering Processor\n")
    print("Send first batch of data on stream: ['Hello world', [3.14, -1,"
          "2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access"
          "!Use ssh instead'}, {'log_level': 'INFO', 'log_message': "
          "'User wil is connected'}], 42, ['Hi', 'five']]"
          )
    data_stream.register_processor(num)
    data_stream.register_processor(text)
    data_stream.register_processor(log)
    print("\n== DataStream statistics ==")
    data_stream.process_stream(stream)
    data_stream.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, CSVPlugin())
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    stream2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]
    print(f"Send another batch of data: {stream2}")
    data_stream.process_stream(stream2)
    print("\n== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, JSONPlugin())
    print("\n== DataStream statistics ==")
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
