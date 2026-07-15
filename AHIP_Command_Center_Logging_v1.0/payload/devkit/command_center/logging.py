from pathlib import Path
from datetime import datetime

class CommandCenterLogger:
    VERSION="1.0.0"

    def __init__(self, repo:Path):
        self.log_dir=repo/"logs"
        self.log_dir.mkdir(parents=True,exist_ok=True)
        self.log_file=self.log_dir/"command_center.log"

    def write(self, level:str, message:str):
        line=f"{datetime.now().isoformat()} [{level}] {message}\n"
        with self.log_file.open("a",encoding="utf-8") as f:
            f.write(line)
        return line

    def info(self,message:str):
        return self.write("INFO",message)

    def error(self,message:str):
        return self.write("ERROR",message)
