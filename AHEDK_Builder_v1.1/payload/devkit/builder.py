"""Allen Hammett Enterprise Development Kit (AHEDK).

Builder controller for orchestrating future AHEDK generator modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Protocol


VERSION = "1.1.0"


class BuildComponent(Protocol):
    """Contract implemented by future AHEDK builder components."""

    def build(self, context: "BuildContext") -> Any:
        ...


@dataclass(frozen=True, slots=True)
class BuildContext:
    project_root: Path
    component_name: str
    requested_at: datetime


class Builder:
    """Coordinates AHEDK build components through one stable interface."""

    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = (project_root or Path.cwd()).resolve()
        self.logger = self._configure_logger()
        self._components: dict[str, BuildComponent] = {}

    @staticmethod
    def _configure_logger() -> logging.Logger:
        logger = logging.getLogger("AHEDK")
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            )
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def register(self, name: str, component: BuildComponent) -> None:
        normalized = name.strip().lower()
        if not normalized:
            raise ValueError("Component name must not be empty.")
        self._components[normalized] = component
        self.logger.info("Registered component: %s", normalized)

    def available_components(self) -> tuple[str, ...]:
        return tuple(sorted(self._components))

    def build(self, component_name: str) -> Any:
        normalized = component_name.strip().lower()
        if normalized not in self._components:
            raise KeyError(
                f"Unknown component '{component_name}'. "
                f"Available: {', '.join(self.available_components()) or 'none'}"
            )

        context = BuildContext(
            project_root=self.project_root,
            component_name=normalized,
            requested_at=datetime.now(timezone.utc),
        )
        self.logger.info("Running component: %s", normalized)
        return self._components[normalized].build(context)

    def status(self) -> dict[str, Any]:
        return {
            "name": "AHEDK",
            "version": VERSION,
            "project_root": str(self.project_root),
            "registered_components": list(self.available_components()),
        }


if __name__ == "__main__":
    builder = Builder()
    print(builder.status())
