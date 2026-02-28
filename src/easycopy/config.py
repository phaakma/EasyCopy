"""Runtime configuration for EasyCopy API."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class RuntimePaths:
    """Represents runtime output directories used by EasyCopy."""

    logs_dir: Path
    changesets_dir: Path

    @classmethod
    def from_inputs(
        cls,
        logs_dir: str | Path | None,
        changesets_dir: str | Path | None,
    ) -> "RuntimePaths":
        """Build runtime paths from optional user inputs."""
        default_logs = Path("./logs")
        default_changesets = Path("./changesets")
        return cls(
            logs_dir=Path(logs_dir) if logs_dir else default_logs,
            changesets_dir=(
                Path(changesets_dir) if changesets_dir else default_changesets
            ),
        )

    def ensure(self) -> None:
        """Ensure runtime directories exist."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.changesets_dir.mkdir(parents=True, exist_ok=True)
