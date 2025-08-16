
from dice_dw.etl import run_pipeline, estimate_revenue_2024
if __name__=="__main__":
    stats = run_pipeline()
    print("Pipeline completed:", stats)
