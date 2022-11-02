import pickle
import pytest
import pandas as pd
from pathlib import Path

import sys
sys.path.append('..')
from utils import get_num_uniq_connections

dir_ = Path(__file__).parent
with open(dir_ / 'cases' / 'uniq_connections_test.pkl', 'rb') as f:
    test_cases = pickle.load(f)


@pytest.mark.parametrize('df,result', test_cases)
def test_uniq_connections(df: pd.DataFrame, result: pd.DataFrame):
    assert result.equals(get_num_uniq_connections(df))
