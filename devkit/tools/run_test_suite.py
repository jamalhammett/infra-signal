from pathlib import Path
import subprocess

class RunTestSuite:
    VERSION="1.0.0"

    def run(self, repo:Path):
        tests=repo/"tests"
        if not tests.exists():
            print("No tests folder found.")
            return
        for test in sorted(tests.glob("test_*.py")):
            print("="*60)
            print(test.name)
            subprocess.run(["python", str(test)], cwd=repo)
