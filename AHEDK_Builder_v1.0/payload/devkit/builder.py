class Builder:
    VERSION="1.0.0"
    def build_all(self,name):
        print("Building",name)
        print("[NEXT] engine_builder")
        print("[NEXT] package_builder")
        print("[NEXT] manifest_builder")
        print("[NEXT] test_builder")
        print("[NEXT] readme_builder")

if __name__=="__main__":
    Builder().build_all("ExampleEngine")
