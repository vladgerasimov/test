import pickle
import pytest
import pandas as pd
from pathlib import Path

import sys
sys.path.append('..')
from utils import group_hotspots_data_by_owner

dir_ = Path(__file__).parent
with open(dir_ / 'cases' / 'grouping_test.pkl', 'rb') as f:
    test_cases = pickle.load(f)


@pytest.mark.parametrize('df,connections,result', test_cases)
def test_uniq_connections(df: pd.DataFrame, connections: pd.DataFrame, result: pd.DataFrame):
    assert result.equals(group_hotspots_data_by_owner(df, connections))
