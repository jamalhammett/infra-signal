from pathlib import Path
from devkit.generators.engine_builder import EngineBuilder
from devkit.generators.package_builder import PackageBuilder
from devkit.tools.run_test_suite import RunTestSuite

class CommandCenterActions:
    VERSION="1.0.0"

    def __init__(self, repo:Path):
        self.repo=repo

    def build_engine(self,name:str):
        return EngineBuilder().build(name,self.repo)

    def package(self,source:Path,target:Path):
        return PackageBuilder().build(source,target)

    def run_tests(self):
        return RunTestSuite().run(self.repo)
