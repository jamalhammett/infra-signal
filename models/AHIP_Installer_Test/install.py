from pathlib import Path
import json, shutil, sys
repo=Path.cwd()
while repo!=repo.parent and not ((repo/'app.py').exists() and (repo/'engines').exists()):
    repo=repo.parent
if not ((repo/'app.py').exists() and (repo/'engines').exists()):
    print('AHIP root not found');sys.exit(1)
pkg=Path(__file__).parent
manifest=json.loads((pkg/'manifest.json').read_text())
for item in manifest['files']:
    src=pkg/item['source']
    dst=repo/item['destination']
    dst.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(src,dst)
    print('[OK]',dst.relative_to(repo))
print('Installation complete.')
