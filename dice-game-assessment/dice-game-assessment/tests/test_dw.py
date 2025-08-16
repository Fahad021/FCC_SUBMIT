
import unittest, pandas as pd
from pathlib import Path
from dice_dw.etl import run_pipeline
WH  = Path(__file__).resolve().parents[1] / "data" / "warehouse"

class TestDW(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_pipeline()
    def test_keys_and_rowcounts(self):
        dim_plan = pd.read_csv(WH/"dim_plan.csv")
        self.assertEqual(dim_plan["plan_id"].isna().sum(), 0)
        self.assertEqual(dim_plan["plan_id"].nunique(), len(dim_plan))
        dim_channel = pd.read_csv(WH/"dim_channel.csv")
        self.assertTrue({"channel_code"}.issubset(set(dim_channel.columns)))
        fact_play = pd.read_csv(WH/"fact_play_session.csv")
        # FK coverage
        self.assertTrue(set(fact_play["channel_code"]).issubset(set(dim_channel["channel_code"])))
        self.assertGreater(len(fact_play), 0)

    def test_dates_and_durations(self):
        fact_play = pd.read_csv(WH/"fact_play_session.csv")
        self.assertEqual(fact_play["start_ts"].isna().sum(), 0)
        self.assertEqual(fact_play["end_ts"].isna().sum(), 0)
        self.assertTrue((fact_play["duration_seconds"]>=0).all())

if __name__=="__main__":
    unittest.main()
