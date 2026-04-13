# scripts/pipeline.py
import sys
import time
sys.path.append('.')

from extract   import extract_all
from transform import run_all_transforms
from load      import load_all
from fcr_transform import run_fcr_transform
def run_pipeline():
    print(" E-COMMERCE ETL PIPELINE STARTED")
    print("=" * 50)

    start = time.time()

    # Step 1 — Extract
    raw_data = extract_all()

    # Step 2 — Transform
    clean_data = run_all_transforms(raw_data)

    # Step 3 — Load
    load_all(clean_data)
    #step 4 run fcr transform and load
    run_fcr_transform()
    
    elapsed = round(time.time() - start, 2)
    print(f"\n PIPELINE COMPLETE in {elapsed} seconds")
    print("=" * 50)
    print(" Open SSMS and query your tables!")

if __name__ == "__main__":
    run_pipeline()