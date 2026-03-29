from abc import ABC, abstractmethod
from typing import Any, List, Protocol, Union, Dict


# ======================
# PROTOCOL (DUCK TYPING)
# ======================
class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


# ======================
# STAGES
# ======================
class InputStage:
    def process(self, data: Any) -> Any:
        return data  # basic validation could go here


class TransformStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, dict):
            data["processed"] = True
        return data


class OutputStage:
    def process(self, data: Any) -> Any:
        return f"Output: {data}"


# ======================
# PIPELINE BASE
# ======================
class ProcessingPipeline(ABC):

    def __init__(self):
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def run(self, data: Any) -> Any:
        try:
            for stage in self.stages:
                data = stage.process(data)
            return data
        except Exception as e:
            return f"Pipeline error: {e}"

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


# ======================
# ADAPTERS
# ======================
class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing JSON data in {self.pipeline_id}")
        return self.run(data)


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing CSV data in {self.pipeline_id}")
        if isinstance(data, str):
            data = data.split(",")
        return self.run(data)


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing Stream data in {self.pipeline_id}")
        return self.run(data)


# ======================
# NEXUS MANAGER
# ======================
class NexusManager:

    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def execute(self, data_list: List[Any]) -> None:
        for pipeline, data in zip(self.pipelines, data_list):
            result = pipeline.process(data)
            print(result)

    def chain(self, data: Any) -> Any:
        print("\nPipeline chaining...")
        for pipeline in self.pipelines:
            data = pipeline.process(data)
        return data


# ======================
# MAIN DEMO
# ======================
def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")

    # Create pipelines
    json_pipeline = JSONAdapter("JSON_PIPE")
    csv_pipeline = CSVAdapter("CSV_PIPE")
    stream_pipeline = StreamAdapter("STREAM_PIPE")

    # Add stages
    for p in [json_pipeline, csv_pipeline, stream_pipeline]:
        p.add_stage(InputStage())
        p.add_stage(TransformStage())
        p.add_stage(OutputStage())

    manager = NexusManager()
    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    # Execute
    manager.execute([
        {"sensor": "temp", "value": 23.5},
        "user,login,logout",
        {"stream": "data"}
    ])

    # Chain pipelines
    result = manager.chain({"value": 100})
    print("\nFinal chained result:", result)


if __name__ == "__main__":
    main()
