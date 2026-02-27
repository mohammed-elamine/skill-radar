from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TextPipelinePaths:
    # Inputs
    jobs_input: Path
    esco_input: Path

    # Outputs
    matches_output: Path
    kpis_output: Path

    def as_spark(self) -> "TextPipelinePathsSpark":
        """Spark wants string paths; keep Path in code, convert at boundary."""
        return TextPipelinePathsSpark(
            jobs_input=str(self.jobs_input),
            esco_input=str(self.esco_input),
            matches_output=str(self.matches_output),
            kpis_output=str(self.kpis_output),
        )


@dataclass(frozen=True)
class TextPipelinePathsSpark:
    jobs_input: str
    esco_input: str
    matches_output: str
    kpis_output: str
