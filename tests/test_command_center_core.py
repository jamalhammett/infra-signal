from pathlib import Path

from devkit.command_center import CommandCenter


def main() -> None:
    center = CommandCenter(Path.cwd())
    status = center.refresh()

    assert status["name"] == "AHIP Command Center"
    assert status["version"] == "1.0.0"
    assert "registry" in status
    assert status["registry"]["total_components"] >= 1

    print("PASS: AHIP Command Center Core")


if __name__ == "__main__":
    main()
