"""AHIP Command Center Core v1.0.

Discovers installed AHEDK generators and tools, exposes their status,
and provides a stable registry for future UI and automation layers.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import importlib
import inspect
from pathlib import Path
from typing import Any


VERSION = "1.0.0"


@dataclass(frozen=True, slots=True)
class CommandComponent:
    name: str
    module: str
    category: str
    version: str
    status: str
    class_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ComponentRegistry:
    """Discovers and reports installed AHEDK components."""

    SEARCH_LOCATIONS = {
        "generator": "devkit.generators",
        "tool": "devkit.tools",
    }

    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = (project_root or Path.cwd()).resolve()
        self._components: dict[str, CommandComponent] = {}

    def discover(self) -> tuple[CommandComponent, ...]:
        self._components.clear()

        for category, package_name in self.SEARCH_LOCATIONS.items():
            package_path = self.project_root / Path(*package_name.split("."))
            if not package_path.exists():
                continue

            for module_file in sorted(package_path.glob("*.py")):
                if module_file.name == "__init__.py":
                    continue

                module_name = f"{package_name}.{module_file.stem}"
                component = self._inspect_module(
                    module_name=module_name,
                    category=category,
                )
                self._components[component.name] = component

        return self.list_components()

    def _inspect_module(
        self,
        *,
        module_name: str,
        category: str,
    ) -> CommandComponent:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            return CommandComponent(
                name=module_name.rsplit(".", 1)[-1],
                module=module_name,
                category=category,
                version="unknown",
                status="error",
            )

        candidate_class = None
        version = getattr(module, "VERSION", None)

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            candidate_class = obj
            version = getattr(obj, "VERSION", version or "unknown")
            break

        return CommandComponent(
            name=module_name.rsplit(".", 1)[-1],
            module=module_name,
            category=category,
            version=str(version or "unknown"),
            status="ready",
            class_name=candidate_class.__name__ if candidate_class else None,
        )

    def list_components(self) -> tuple[CommandComponent, ...]:
        return tuple(
            sorted(
                self._components.values(),
                key=lambda item: (item.category, item.name),
            )
        )

    def get(self, name: str) -> CommandComponent | None:
        return self._components.get(name)

    def summary(self) -> dict[str, Any]:
        components = self.list_components()
        return {
            "version": VERSION,
            "project_root": str(self.project_root),
            "total_components": len(components),
            "ready": sum(item.status == "ready" for item in components),
            "errors": sum(item.status == "error" for item in components),
            "components": [item.to_dict() for item in components],
        }


class CommandCenter:
    """Core orchestration facade for the AHIP Command Center."""

    VERSION = VERSION

    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = (project_root or Path.cwd()).resolve()
        self.registry = ComponentRegistry(self.project_root)

    def refresh(self) -> dict[str, Any]:
        self.registry.discover()
        return self.status()

    def status(self) -> dict[str, Any]:
        return {
            "name": "AHIP Command Center",
            "version": self.VERSION,
            "registry": self.registry.summary(),
        }


if __name__ == "__main__":
    center = CommandCenter()
    print(center.refresh())
