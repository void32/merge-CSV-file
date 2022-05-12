from lib2to3.pgen2.token import COLON
import os
from typing import Dict

import pandas as pd
from pandas import DataFrame

DIR = os.path.abspath(os.path.join(__file__, os.pardir))
DATA_DIR = os.path.join(DIR, 'data')

OUTPUT_NAME = 'merged.csv'

COLON_TO_MERGE_ON = ["East","North"]

def load(data_dir: str) -> Dict[str, DataFrame]:
    data_frames = {}
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        data_frames[filename] = pd.read_csv(filepath)
    
    return data_frames


def merge(data_frames: Dict[str, DataFrame]) -> DataFrame:
    df_merged = None
    for name,df in data_frames.items():
        if df_merged is None:
            df_merged = df
        else:
            df_merged = df_merged.merge(df, on=COLON_TO_MERGE_ON)

    return df_merged


if __name__ == "__main__":
    merge(load(DATA_DIR)).to_csv(os.path.join(DIR, OUTPUT_NAME), index = None, header=True)
