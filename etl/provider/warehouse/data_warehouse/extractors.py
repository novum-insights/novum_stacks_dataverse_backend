from etl.common.loaders import load_from_data_warehouse


def get_existing_stacks_timestamps():
    sql = "SELECT unix_timestamp FROM stx_historical_prices"
    return load_from_data_warehouse(sql, params={})
