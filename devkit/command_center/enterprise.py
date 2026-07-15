from pathlib import Path
from .core import CommandCenter
from .actions import CommandCenterActions
from .logging import CommandCenterLogger
from .ui import CommandCenterUI

class EnterpriseCommandCenter:
    VERSION="1.0.0"

    def __init__(self, repo:Path):
        self.repo=repo
        self.core=CommandCenter(repo)
        self.actions=CommandCenterActions(repo)
        self.logger=CommandCenterLogger(repo)
        self.ui=CommandCenterUI(repo)

    def startup(self):
        self.logger.info("Command Center Started")
        return self.core.refresh()

    def dashboard(self):
        return self.ui.dashboard()
