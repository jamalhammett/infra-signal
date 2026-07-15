from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

class PackageBuilder:
    VERSION="1.0.0"

    def build(self, source_folder:Path, output_zip:Path):
        with ZipFile(output_zip,"w",ZIP_DEFLATED) as z:
            for f in source_folder.rglob("*"):
                if f.is_file():
                    z.write(f,f.relative_to(source_folder))
        return output_zip
