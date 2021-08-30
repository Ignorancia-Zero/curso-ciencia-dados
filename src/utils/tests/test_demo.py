import os
import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent.parent))


class TestTemplate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_path = Path(os.path.dirname(__file__))

    def test_nada(self) -> None:
        self.assertEqual(True, True)


if __name__ == "__main__":
    unittest.main()
