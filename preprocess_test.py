import os
import glob
import json
from absl.testing import absltest

from preprocess import preprocess


class UnitTests(absltest.TestCase):
    def test_preprocess_test(self):
        for filename in [os.path.splitext(os.path.basename(f))[0] for f in glob.glob("test_data/*.json")]:
            with open(os.path.join("test_data", f"{filename}.json")) as f:
                obj = json.load(f)
            with open(os.path.join("test_data", f"{filename}.gold.txt")) as f:
                gold = f.read()

            preprocessed = [doc for doc in preprocess(obj)]
            self.assertEqual("\n".join(preprocessed), gold)


if __name__ == '__main__':
    absltest.main()
