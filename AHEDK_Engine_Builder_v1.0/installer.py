from pathlib import Path
import shutil,json
cfg=Path.home()/".ahip-devkit.json"
if not cfg.exists():
    raise SystemExit("Install AHEDK Builder first.")
repo=Path(json.loads(cfg.read_text())["repository"])
m=json.loads((Path(__file__).parent/"manifest.json").read_text())
for f in m["files"]:
 s=Path(__file__).parent/f["source"]
 d=repo/f["destination"]
 d.parent.mkdir(parents=True,exist_ok=True)
 shutil.copy2(s,d)
 print("[OK]",d)
print("Engine Builder installed.")
