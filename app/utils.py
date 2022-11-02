import pandas as pd
import numpy as np
import datetime

today = datetime.datetime.today()


def get_num_uniq_connections(connections: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate and number of unique connections to hotspot_id
    :param connections: pandas DataFrame with columns hotspot_id, connected_at and installation_id
    :return: pandas DataFrame with calculated results
    """

    connections['connected_at'] = pd.to_datetime(connections['connected_at']).dt.tz_localize(None)
    connections['connected_days_ago'] = (today - connections['connected_at']).dt.days
    connections['connected_within_week'] = (connections['connected_days_ago'] <= 7).astype(int)
    connections['connected_within_month'] = (connections['connected_days_ago'] <= 30).astype(int)
    connections['connected_within_year'] = (connections['connected_days_ago'] <= 365).astype(int)

    uniq_connections_within_week = connections[connections['connected_within_week'] == 1].groupby('hotspot_id').agg({
        'installation_id': lambda x: len(set(x))
    }).rename(columns={'installation_id': 'uniq_connections_within_week'})

    uniq_connections_within_month = connections[connections['connected_within_month'] == 1].groupby('hotspot_id').agg({
        'installation_id': lambda x: len(set(x))
    }).rename(columns={'installation_id': 'uniq_connections_within_month'})

    uniq_connections_within_year = connections[connections['connected_within_year'] == 1].groupby('hotspot_id').agg({
        'installation_id': lambda x: len(set(x))
    }).rename(columns={'installation_id': 'uniq_connections_within_year'})

    uniq_connections_all_time = connections.groupby('hotspot_id').agg({
        'installation_id': lambda x: len(set(x))
    }).rename(columns={'installation_id': 'uniq_connections'})

    uniq_connections = (uniq_connections_all_time
                        .join(uniq_connections_within_year)
                        .join(uniq_connections_within_month)
                        .join(uniq_connections_within_week)
                        .fillna(0).astype(int))

    return uniq_connections


def group_hotspots_data_by_owner(hotspots: pd.DataFrame, connections: pd.DataFrame) -> pd.DataFrame:
    """
    Groups data about hotspots by its owner and calculates required aggregates
    :param hotspots: pandas DataFrame containing hotspots data
    :param connections: pandas DataFrame containing connections data
    :return: pandas DataFrame containing required aggregates
    """

    uniq_connections = get_num_uniq_connections(connections)

    hotspots = hotspots.set_index('id').join(uniq_connections, how='left').reset_index()

    for col in ['uniq_connections', 'uniq_connections_within_year', 'uniq_connections_within_month',
                'uniq_connections_within_week']:
        hotspots[col].fillna(0, inplace=True)
        hotspots[col] = hotspots[col].astype(int)

    hotspots['is_place'] = (hotspots['foursquare_id'].notna() | hotspots['google_place_id'].notna()).astype(int)
    hotspots['created_at'] = pd.to_datetime(hotspots['created_at']).dt.tz_localize(None)

    hotspots['created_days_ago'] = (today - hotspots['created_at']).dt.days
    hotspots['created_within_week'] = (hotspots['created_days_ago'] <= 7).astype(int)
    hotspots['created_within_month'] = (hotspots['created_days_ago'] <= 30).astype(int)

    hotspots['is_good'] = (hotspots['score_v4'] > 0.6).astype(int)
    hotspots['is_average'] = ((0.3 < hotspots['score_v4']) & (hotspots['score_v4'] <= 0.6)).astype(int)
    hotspots['is_bad'] = (hotspots['score_v4'] <= 0.3).astype(int)

    for col in ['uniq_connections', 'uniq_connections_within_year',
                'uniq_connections_within_month', 'uniq_connections_within_week']:
        for num in [1, 5, 10]:
            hotspots[f'{col}>{num}'] = (hotspots[col] > num).astype(int)

    result = hotspots.groupby('owner_id').agg({'id': 'count',
                                               'is_place': np.sum,
                                               'created_within_month': np.sum,
                                               'created_within_week': np.sum,
                                               'is_good': np.sum,
                                               'is_average': np.sum,
                                               'is_bad': np.sum,
                                               'uniq_connections>1': np.sum,
                                               'uniq_connections>5': np.sum,
                                               'uniq_connections>10': np.sum,
                                               'uniq_connections_within_year>1': np.sum,
                                               'uniq_connections_within_year>5': np.sum,
                                               'uniq_connections_within_year>10': np.sum,
                                               'uniq_connections_within_month>1': np.sum,
                                               'uniq_connections_within_month>5': np.sum,
                                               'uniq_connections_within_month>10': np.sum,
                                               'uniq_connections_within_week>1': np.sum,
                                               'uniq_connections_within_week>5': np.sum,
                                               'uniq_connections_within_week>10': np.sum
                                               }).rename(columns={'id': 'hotspots_created'})

    return result

