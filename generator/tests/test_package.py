import sys
import unittest


class PackageTests(unittest.TestCase):
    def test_package_imports_without_other_workspace_members(self):
        import datagen  # noqa: F401

        loaded = {name.split(".")[0] for name in sys.modules}
        self.assertFalse(loaded & {"platform_app", "lakehouse"})


if __name__ == "__main__":
    unittest.main()
