from pathlib import Path

class TestBuilder:
    VERSION="1.0.0"

    def build(self, engine_name:str, tests_root:Path):
        name=engine_name.strip().lower().replace(" ","_")
        tests_root.mkdir(parents=True,exist_ok=True)
        test_file=tests_root/f"test_{name}_engine.py"
        test_file.write_text(f"""import unittest

class Test{engine_name.replace(' ','')}Engine(unittest.TestCase):
    def test_placeholder(self):
        self.assertTrue(True)

if __name__=="__main__":
    unittest.main()
""",encoding="utf-8")
        return test_file
