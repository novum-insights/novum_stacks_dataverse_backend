import pandas as pd
import numpy as np
import logging
import datetime
import statistics
from functools import partial
from etl.common.paths import STACKS_DATA_BUCKET_NAME
from etl.provider.cryptocompare.extractors import get_cryptocurrency_to_usd_conversion_at_ts
from botocore.exceptions import ClientError
from requests import HTTPError

DEFAULT_DATAVERSE_POLICY = 'public-read'

POOL_NAMES = {
    'SP269PKK91NTCGH93RPKTT8AZ6V6ZKH7A4Q7N5F7B': 'Friedger Stacker',
    'SP3HXJJMJQ06GNAZ8XWDN1QM48JEDC6PP6W3YZPZJ': 'OKCoin',
    'SP1GB55RVXR2RK3HP8GTSFAMT3RTYXQ15AGS8YB3T': 'Stacks Korea Community Pool',
    'SP700C57YJFD5RGHK0GN46478WBAM2KG3A4MN2QJ': 'Friedger Pool',
    'SP6K6QNYFYF57K814W4SHNGG23XNYYE1B8NV0G1Y': 'Friedger Pool 2'
}

STX_2_START = datetime.datetime(2021, 1, 14, 0, 0, 0).timestamp()
MAXVALUE_TS = 2147483647

WHALE_BALANCE_MIN_VALUE = 10000
WHALE_BALANCE_MAX_VALUE = 10000000

SIGNIFICANT_DIGITS = 3


def _convert_datestring_to_unix_ts(date_string):
    return datetime.datetime.fromisoformat(date_string).timestamp()


def _apply_type_id_conversion(value):
    if value == 1:
        return 'transfer'
    elif value == 2:
        return 'mint'
    else:
        return 'burn'


def _apply_time_conversion(value):
    return datetime.datetime.strftime(datetime.datetime.fromtimestamp(value), '%Y-%m-%d %H:%M:%S')

def _to_timestamp(value):
    return int(value.timestamp())

def _calculate_usd_value(df, colname_dest, colname_source, source_symbol):
    result = []
    for i, row in df.iterrows():
        result.append(get_cryptocurrency_to_usd_conversion_at_ts(source_symbol, int(row['burn_block_time']))
                      * row[colname_source])
    df[colname_dest] = result
    return df


def run_get_daily_transactions_over_time(execution_date):
    from etl.provider.stacks.extractors import extract_txs_data_between_timestamps
    from etl.provider.stacks.paths import STX_DAILY_TXS_OVER_TIME_PATH
    from etl.common.loaders import save_obj
    from etl.common.helpers import get_every_nth_index

    execution_ts = _convert_datestring_to_unix_ts(execution_date)
    search_ts = STX_2_START
    key = STX_DAILY_TXS_OVER_TIME_PATH.format(execution_date=execution_date)
    result = []
    while search_ts <= execution_ts:
        # daily_dt = datetime.datetime.fromtimestamp(search_ts)
        # day_string = datetime.datetime.strftime(daily_dt, '%Y-%m-%d')
        tx_count = extract_txs_data_between_timestamps(search_ts, search_ts + 86400)
        # result[day_string] = tx_count
        result.append((int(search_ts), tx_count))
        search_ts += 86400
    indices = get_every_nth_index(result, 2)

    save_obj({result[e][0]: result[e][1] for e in indices}, key, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True)


def run_get_daily_address_growth_over_time(execution_date):
    from etl.common.loaders import save_obj
    from etl.common.helpers import get_every_nth_index
    from etl.provider.stacks.paths import STX_ADDRESSES_GROWTH_OVER_TIME_PATH
    from etl.provider.stacks.extractors import extract_block_heights_between_timestamps,\
        extract_stx_event_parties_between_blocks
    execution_ts = _convert_datestring_to_unix_ts(execution_date)
    search_ts = STX_2_START
    address_set = set()

    result = []

    while search_ts <= execution_ts:
        # daily_dt = datetime.datetime.fromtimestamp(search_ts)
        # day_string = datetime.datetime.strftime(daily_dt, '%Y-%m-%d')
        blocks = extract_block_heights_between_timestamps(search_ts, search_ts + 86400)

        if len(blocks) > 0:
            start_block = blocks[0]
            end_block = blocks[-1]
            events = extract_stx_event_parties_between_blocks(start_block, end_block)

            for _, row in events.iterrows():
                if row['sender'] is not None:
                    address_set.add(row['sender'])
                if row['recipient'] is not None:
                    address_set.add(row['recipient'])

        result.append((int(search_ts),len(address_set)))
        search_ts += 86400
    indices = get_every_nth_index(result, 2)
    save_obj({result[e][0]: result[e][1] for e in indices}, STX_ADDRESSES_GROWTH_OVER_TIME_PATH.format(execution_date=execution_date),
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True)
    return list(address_set)


def run_get_daily_active_addresses_over_time(execution_date):
    from etl.common.loaders import save_obj
    from etl.common.helpers import get_every_nth_index
    from etl.provider.stacks.paths import STX_ADDRESSES_DAILY_ACTIVE_OVER_TIME_PATH
    from etl.provider.stacks.extractors import extract_block_heights_between_timestamps, \
        extract_stx_event_parties_between_blocks
    execution_ts = _convert_datestring_to_unix_ts(execution_date)
    search_ts = STX_2_START

    result = []

    while search_ts <= execution_ts:
        address_set = set()

        # daily_dt = datetime.datetime.fromtimestamp(search_ts)
        # day_string = datetime.datetime.strftime(daily_dt, '%Y-%m-%d')
        blocks = extract_block_heights_between_timestamps(search_ts, search_ts + 86400)

        if len(blocks) > 0:
            start_block = blocks[0]
            end_block = blocks[-1]
            events = extract_stx_event_parties_between_blocks(start_block, end_block)

            for _, row in events.iterrows():
                if row['sender'] is not None:
                    address_set.add(row['sender'])
                if row['recipient'] is not None:
                    address_set.add(row['recipient'])

        result.append((int(search_ts),len(address_set)))
        search_ts += 86400
    indices = get_every_nth_index(result, 2)

    save_obj({result[e][0]: result[e][1] for e in indices}, STX_ADDRESSES_DAILY_ACTIVE_OVER_TIME_PATH.format(execution_date=execution_date),
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True)


def run_get_historical_price_data_over_time():
    from etl.provider.coingecko.api.coins.extractors import get_hourly_market_chart_per_coin
    from etl.provider.warehouse.data_warehouse.extractors import get_existing_stacks_timestamps
    from etl.common.loaders import save_df_to_db
    prices = get_hourly_market_chart_per_coin('blockstack')['blockstack']['prices']
    prices = [[t // 1000, p] for [t, p] in prices]
    prices_ordered = sorted(prices, key=lambda x: x[0])
    df = pd.DataFrame({'unix_timestamp': [x[0] for x in prices_ordered], 'price_usd': [x[1] for x in prices_ordered]})

    existing_ts = list(get_existing_stacks_timestamps()['unix_timestamp'])
    df = df[~df['unix_timestamp'].isin(existing_ts)]
    save_df_to_db(df, 'stx_historical_prices', index=False, if_exists='append')


def run_distribution_of_wallet_balances(execution_date, addresses_list):
    from etl.provider.stacks.extractors import extract_balance_from_single_address
    from etl.provider.stacks.paths import STX_WALLET_BALANCES_PATH, STX_RAW_WALLET_BALANCES_PATH
    from etl.common.loaders import save_obj
    from collections import Counter

    balances = []
    whales = []
    nonzero_bals = 0
    now_ts = int(datetime.datetime.combine(datetime.datetime.strptime(execution_date, '%Y-%m-%d'),
                                       datetime.datetime.min.time()).timestamp())
    for stx_address in addresses_list:
        # TODO use numpy
        bal = extract_balance_from_single_address(stx_address)
        if bal > 0:
            nonzero_bals += 1
        if bal >= WHALE_BALANCE_MIN_VALUE:
            balances.append(bal)
            whales.append((now_ts, stx_address, bal))
    save_obj({'whale_balances': balances, 'nonzero_bals': nonzero_bals},
             STX_RAW_WALLET_BALANCES_PATH.format(execution_date=execution_date),
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True)
    bins = range(WHALE_BALANCE_MIN_VALUE, WHALE_BALANCE_MAX_VALUE, 500)
    binned = np.digitize(np.array(balances), bins).tolist()
    distribution = dict(Counter(binned))
    result = {WHALE_BALANCE_MIN_VALUE + key*500: value for (key, value) in distribution.items()}
    save_obj(result, STX_WALLET_BALANCES_PATH.format(execution_date=execution_date),
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True)
    return whales


def run_save_whale_balances_history(records):
    from etl.common.loaders import save_df_to_db
    from etl.provider.stacks.extractors import delete_existing_whale_balances_at_timestamp

    df_new_records = pd.DataFrame(records, columns=['timestamp', 'whale_address', 'balance'])
    ts = df_new_records.iloc[0]['timestamp']
    delete_existing_whale_balances_at_timestamp(ts.item())
    save_df_to_db(df_new_records, 'whale_historical_stx_balances', index=False, if_exists='append')


def run_get_stx_stacked_over_time(execution_date):
    from etl.provider.stacks.extractors import extract_stx_reward_cycle_details,\
        determine_current_stx_reward_cycle
    from etl.provider.stacks.paths import STX_STACKED_OVER_TIME_PATH
    from etl.common.loaders import save_obj
    current_stx_cycle, _ = determine_current_stx_reward_cycle()
    stx_stacked_over_time = extract_stx_reward_cycle_details(current_stx_cycle + 1)['cumulativeStacked']
    result_dict = {elem['x']: elem['y'] for elem in stx_stacked_over_time}
    result = {key//1000: value * pow(10, -6)
              for (key, value) in result_dict.items()}
    save_obj(result, STX_STACKED_OVER_TIME_PATH.format(execution_date=execution_date),
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True)


def run_copy_stacks_dataset_to_latest(execution_date, path_prefix):
    from etl.common.loaders import copy_to_latest

    copy_to_latest(path_prefix, execution_date, source_bucket_name=STACKS_DATA_BUCKET_NAME,
                   dest_bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_create_stx_spot_stats(execution_date):
    from etl.provider.stacks.extractors import extract_txs_data_between_timestamps,\
        determine_current_stx_reward_cycle,extract_latest_stx_whale_movement, get_fastfee_in_mstx, \
        extract_stx_api_general_info, get_circulating_supply, extract_apy, extract_latest_transfer_fee
    from etl.provider.stacks.paths import STX_RAW_WALLET_BALANCES_PATH, STX_SPOT_STATS_PATH, STX_MEMPOOL_PATH,\
        STX_SPOT_STATS_HISTORICAL_PATH
    from etl.provider.coingecko.api.coins.extractors import get_coin_details_with_spot_market
    from etl.provider.cryptocompare.extractors import get_cryptocurrency_to_usd_conversion_at_ts
    from etl.common.loaders import get_json, save_obj

    def gini(array):
        """Calculate the Gini coefficient of a numpy array."""
        # based on bottom eq: http://www.statsdirect.com/help/content/image/stat0206_wmf.gif
        # from: http://www.statsdirect.com/help/default.htm#nonparametric_methods/gini.htm
        array = array.flatten()  # all values are treated equally, arrays must be 1d
        if np.amin(array) < 0:
            array -= np.amin(array)  # values cannot be negative
        array += 0.0000001  # values cannot be 0
        array = np.sort(array)  # values must be sorted
        index = np.arange(1, array.shape[0] + 1)  # index per array element
        n = array.shape[0]  # number of array elements
        return (np.sum((2 * index - n - 1) * array)) / (n * np.sum(array))  # Gini coefficient

    now_ts = int(datetime.datetime.now().timestamp())

    current_block = extract_stx_api_general_info()["stacks_tip_height"]
    total_txs_pastday = extract_txs_data_between_timestamps(now_ts - 86400, now_ts)
    total_txs_alltime = extract_txs_data_between_timestamps(0, MAXVALUE_TS)

    current_stx_cycle, current_cycle_info = determine_current_stx_reward_cycle()
    total_stackers = current_cycle_info['totalStackers']
    total_stacked_stx = round(current_cycle_info['totalStacked'] * pow(10, -6), SIGNIFICANT_DIGITS)
    total_stacked_usd = round(get_cryptocurrency_to_usd_conversion_at_ts('STX', now_ts) * total_stacked_stx,
                              SIGNIFICANT_DIGITS)
    slot_minimum = current_cycle_info['minimumThreshold']
    opening_stx_price = round(current_cycle_info['historicalPrices'][str(current_cycle_info['cycleStartHeight'])]['stx'], 2)
    blocks_left = current_cycle_info['blocks_left']
    cycle_phase = current_cycle_info['current_phase']

    apy = extract_apy()

    bals_and_nonzero = get_json(STX_RAW_WALLET_BALANCES_PATH.format(execution_date='latest'), raw=True,
                    bucket_name=STACKS_DATA_BUCKET_NAME)
    balance_list = bals_and_nonzero['whale_balances']

    whales = [x for x in balance_list if x >= 100000]
    whale_percent = (len(whales) / len(balance_list))*100
    balance_list = np.array(balance_list)
    gini_coefficient = round(float(gini(balance_list)), 2)
    median_balance = float(np.median(balance_list))

    latest_whale = extract_latest_stx_whale_movement()

    coin_details = get_coin_details_with_spot_market(['blockstack'])['blockstack']
    ath = coin_details["market_data"]["ath"]['usd']
    perc_from_ath = round(coin_details["market_data"]["ath_change_percentage"]['usd'], 2)

    stx_fee = get_fastfee_in_mstx() * pow(10, -6)
    usd_fee = get_cryptocurrency_to_usd_conversion_at_ts('STX', now_ts) * stx_fee

    latest_transfer_fee = round(extract_latest_transfer_fee() * pow(10, -6), SIGNIFICANT_DIGITS)

    mempool_data = get_json(STX_MEMPOOL_PATH, raw=True, bucket_name=STACKS_DATA_BUCKET_NAME)
    memdatas = [(int(k), v) for k,v in mempool_data.items()]
    memdata = sorted(memdatas, key=lambda x: x[0])[-1]

    result = {
        'refresh_ts': now_ts,
        'total_txs_alltime': total_txs_alltime,
        'total_txs_past24': total_txs_pastday,
        'current_cycle_no': current_stx_cycle,
        'current_cycle_open_price': opening_stx_price,
        'cycle_blocks_left': blocks_left,
        'current_cycle_phase': cycle_phase,
        'total_stackers': total_stackers,
        'total_stacked': total_stacked_stx,
        'total_stacked_usd': total_stacked_usd,
        'slot_minimum': slot_minimum,
        'current_stx_block_height': current_block,
        'stacking_apy': apy,
        'gini_coefficient': gini_coefficient,
        'median_balance': median_balance,
        'nonzero_bal_addx': bals_and_nonzero['nonzero_bals'],
        'whale_percent': whale_percent,
        'latest_whale_amount': round(int(latest_whale['token_transfer_amount']) * pow(10, -6), 2),
        'latest_whale_timestamp': int(latest_whale['burn_block_time']),
        'ath': ath,
        'perc_from_ath': perc_from_ath,
        'mstx_fee': stx_fee,  # stx/byte
        'usd_fee': usd_fee,  # stx/byte
        'latest_transfer_fee': latest_transfer_fee,  # stx
        'now_in_mempool': round(memdata[1], 8),
        'circulating_supply': round(get_circulating_supply(), SIGNIFICANT_DIGITS),
    }

    save_obj(result, STX_SPOT_STATS_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)

    result['stacking_apy'] = None if result['stacking_apy'] == '-.-%' else float(result['stacking_apy'].replace('%', ''))
    execution_ts = str(int(_convert_datestring_to_unix_ts(execution_date)))
    historical_datapoints = ['median_balance', 'whale_percent', 'stacking_apy']
    historical_dict = get_json(STX_SPOT_STATS_HISTORICAL_PATH, raw=True, bucket_name=STACKS_DATA_BUCKET_NAME)
    if execution_ts not in historical_dict.keys():
        historical_dict[execution_ts] = {k: result[k] for k in historical_datapoints}
    save_obj(historical_dict, STX_SPOT_STATS_HISTORICAL_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def extract_stx_price_over_time_intervals(execution_date):
    from etl.provider.stacks.extractors import extract_stx_price_from_ts
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_PRICE_OVER_TIME_7D_PATH, STX_PRICE_OVER_TIME_30D_PATH, \
        STX_PRICE_OVER_TIME_90D_PATH

    end_date = datetime.date.fromisoformat(execution_date)

    dt7_ts = int(datetime.datetime.combine((end_date - datetime.timedelta(days=6)),
                                       datetime.datetime.min.time()).timestamp())
    dt30_ts = int(datetime.datetime.combine((end_date - datetime.timedelta(days=29)),
                                        datetime.datetime.min.time()).timestamp())
    dt90_ts = int(datetime.datetime.combine((end_date - datetime.timedelta(days=89)),
                                        datetime.datetime.min.time()).timestamp())

    d7_price_df = extract_stx_price_from_ts(dt7_ts)
    d30_price_df = extract_stx_price_from_ts(dt30_ts)
    d90_price_df = extract_stx_price_from_ts(dt90_ts)

    # d7_price_df['time_string'] = d7_price_df['unix_timestamp'].apply(_apply_time_conversion)
    # d30_price_df['time_string'] = d30_price_df['unix_timestamp'].apply(_apply_time_conversion)
    # d90_price_df['time_string'] = d90_price_df['unix_timestamp'].apply(_apply_time_conversion)

    d7_result = {str(int(row['unix_timestamp'])): round(row['price_usd'], SIGNIFICANT_DIGITS) for _,row in d7_price_df.iterrows()}
    d30_result = {str(int(row['unix_timestamp'])): round(row['price_usd'], SIGNIFICANT_DIGITS) for _,row in d30_price_df.iterrows()}
    d90_result = {str(int(row['unix_timestamp'])): round(row['price_usd'], SIGNIFICANT_DIGITS) for _,row in d90_price_df.iterrows()}

    save_obj(d7_result, STX_PRICE_OVER_TIME_7D_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)
    save_obj(d30_result, STX_PRICE_OVER_TIME_30D_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)
    save_obj(d90_result, STX_PRICE_OVER_TIME_90D_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_mining_data_last100():
    from etl.provider.stacks.extractors import extract_stx_api_general_info, extract_mining_info_per_block_number\
        , extract_mining_overview_data
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_MININGDATA_LAST100_PATH

    current_block = extract_stx_api_general_info()["stacks_tip_height"]
    lookback_block = current_block - 100

    btc_burns = {}
    btc_burns_winner_only = {}
    stx_rewards = {}
    win_probabilities = {}
    winning_miners = {}
    block_fees = []

    for blk in range(lookback_block, current_block + 1):
        res = extract_mining_info_per_block_number(blk)
        btc_burns[blk] = round(res['total_burn_fee'] * pow(10, -8), SIGNIFICANT_DIGITS)

        winning_miner = res['winner_stx_address']
        winning_miners[blk] = winning_miner

        winner_burn_fee = [m['commit_value'] for m in res['blockCommits'] if m['is_winner']][0]
        btc_burns_winner_only[blk] = winner_burn_fee * pow(10, -8)

        win_probability = round(winner_burn_fee / res['total_burn_fee'], SIGNIFICANT_DIGITS)
        win_probabilities[blk] = win_probability

        stx_rewards[blk] = res['block_reward']
        block_fees.append(res['tx_reward'])

    r_totalbtc = extract_mining_overview_data()
    total_btc_committed = r_totalbtc['btcSpentAllTime']['aggregate']['sum']['commit_value'] +\
                          r_totalbtc['btcFeesAllTime']['aggregate']['sum']['commit_btc_gas_fee']
    total_btc_committed = round(total_btc_committed * pow(10, -8), SIGNIFICANT_DIGITS)
    avg_btc_committed = round(sum(btc_burns_winner_only.values()) / len(btc_burns_winner_only.keys()), SIGNIFICANT_DIGITS)
    avg_fee_per_block = round(statistics.mean(block_fees), SIGNIFICANT_DIGITS)

    save_obj({'btc_burns': btc_burns, 'stx_rewards': stx_rewards, 'win_probabilities': win_probabilities,
              'last_block_reward': list(stx_rewards.values())[-1],
            'total_btc_committed': total_btc_committed, 'avg_btc_committed': avg_btc_committed,
            'winner_burn_fees': btc_burns_winner_only, 'avg_fee_per_block': avg_fee_per_block,
            'winning_miners': winning_miners, 'total_stx_rewards': sum(stx_rewards.values()),
            'block_heights': list(range(lookback_block, current_block + 1))}, STX_MININGDATA_LAST100_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_calculate_mining_risk_reward_comparison():
    from etl.provider.stacks.extractors import get_block_times
    from etl.common.loaders import save_obj, get_json
    from etl.provider.stacks.paths import STX_MININGDATA_LAST100_PATH, STX_MINING_RISKREWARD_COMPARISON_PATH
    source_data = get_json(STX_MININGDATA_LAST100_PATH, raw=True,
                            bucket_name=STACKS_DATA_BUCKET_NAME)
    spent = source_data['winner_burn_fees']
    obtained = source_data['stx_rewards']
    blocks = source_data['block_heights']
    df_blocktimes = get_block_times(blocks[0], blocks[-1])
    df_mining = pd.DataFrame({'block_height': list(spent.keys()),
                              'spent_btc': list(spent.values()),
                              'rewarded_stx': list(obtained.values())})
    df_mining['block_height'] = pd.to_numeric(df_mining['block_height'])
    df_final = (pd.merge(df_blocktimes, df_mining, on='block_height')
               .pipe(partial(_calculate_usd_value, colname_dest='spent_usd', colname_source='spent_btc', source_symbol='BTC'))
               .pipe(partial(_calculate_usd_value, colname_dest='rewarded_usd', colname_source='rewarded_stx', source_symbol='STX')))
    save_obj(df_final.drop(columns=['spent_btc', 'rewarded_stx']).set_index('block_height').to_dict('index'),
             STX_MINING_RISKREWARD_COMPARISON_PATH,
             replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_most_successful_miners():
    from etl.common.loaders import save_obj, get_json
    from etl.provider.stacks.paths import STX_MININGDATA_LAST100_PATH, STX_MINING_MOST_SUCCESSFUL_PATH
    from collections import Counter
    source_data = get_json(STX_MININGDATA_LAST100_PATH, raw=True,
                           bucket_name=STACKS_DATA_BUCKET_NAME)['winning_miners']
    save_obj(dict(Counter(source_data.values())),STX_MINING_MOST_SUCCESSFUL_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_stacking_reward_per_block_last100():
    from etl.provider.stacks.extractors import determine_current_stx_reward_cycle,\
        extract_stx_reward_details_per_cycle, get_btc_block_times
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_STACKING_REWARDS_LAST100_PATH, USD_STACKING_REWARDS_LAST20

    current_cycle, _ = determine_current_stx_reward_cycle()

    rewards = {}
    for c in [current_cycle, current_cycle - 1]:
        rewards.update(extract_stx_reward_details_per_cycle(c))

    result = {k: (v['total_amount'] * pow(10, -8)) for k, v in rewards.items()}
    save_obj(result, STX_STACKING_REWARDS_LAST100_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=DEFAULT_DATAVERSE_POLICY)

    btc_blocks = sorted([int(i) for i in result.keys()])[-20:]
    block_times = get_btc_block_times(btc_blocks[0], btc_blocks[-1])
    btc_blocks = [str(b) for b in btc_blocks]

    result_dict_small = {int(k): v for k, v in result.items() if k in btc_blocks}
    result_df = pd.DataFrame({'block_height': result_dict_small.keys(), 'rewarded_btc': result_dict_small.values()})
    df_final_mstx = pd.merge(block_times, result_df, on='block_height')
    df_final_usd = (df_final_mstx
                    .pipe(partial(_calculate_usd_value, colname_dest='rewarded_usd',
                                  colname_source='rewarded_btc', source_symbol='BTC')))

    save_obj({str(int(row['block_height'])): round(row['rewarded_usd'].item(), SIGNIFICANT_DIGITS) for _, row in df_final_usd.iterrows()},
             USD_STACKING_REWARDS_LAST20, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_nft_trade_volume_stx_per_block():
    from etl.provider.stacks.extractors import extract_nft_total_trade_volume_stx_per_block, get_block_times
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_NFT_TRADE_VOLUME_STX_PER_BLOCK_PATH, STX_NFT_TRADE_VOLUME_STX_PER_DAY_PATH
    data = extract_nft_total_trade_volume_stx_per_block()['columns']

    blocks = data['height']
    vols = data['volume']
    save_obj({blocks[i]: vols[i] for i in range(len(blocks))}, STX_NFT_TRADE_VOLUME_STX_PER_BLOCK_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME, acl_policy=DEFAULT_DATAVERSE_POLICY)

    block_times = get_block_times(min(blocks), max(blocks))
    volumes = pd.DataFrame({'block_height': blocks, 'volumes': vols})
    df_partial = pd.merge(block_times, volumes, on='block_height')
    df_partial['date'] = pd.to_datetime(df_partial['burn_block_time'], unit='s').dt.date
    series_partial = df_partial.groupby('date')['volumes'].sum()
    # series_partial.index = series_partial.index.strftime('%Y-%m-%d')
    series_partial.index = pd.to_datetime(series_partial.index).strftime('%Y-%m-%d')


    save_obj({str(int(datetime.datetime.fromisoformat(i).timestamp())): v for i, v in series_partial.items()},
             STX_NFT_TRADE_VOLUME_STX_PER_DAY_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
            acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_top_nfts_by_trade_vol_last100():
    from etl.provider.stacks.extractors import extract_stacksonchain_raw_data, SOC_TOPTRADED_VOL_LAST100_URL
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_NFT_TOPTRADED_LAST100_PATH
    data = extract_stacksonchain_raw_data(SOC_TOPTRADED_VOL_LAST100_URL)
    result = {row['NFT']: {'value': row['Volume'], 'contract': row['redirect']} for _, row in data.iterrows()}
    save_obj(result,
             STX_NFT_TOPTRADED_LAST100_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_top_nfts_by_mints_last100():
    from etl.provider.stacks.extractors import extract_stacksonchain_raw_data, SOC_TOPMINTS_LAST100_URL
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_NFT_TOPMINTED_LAST100_PATH
    df_vols = extract_stacksonchain_raw_data(SOC_TOPMINTS_LAST100_URL)
    result = {row['contract']: {'value': row['mints'], 'contract': row['redirect']} for _, row in df_vols.iterrows()}
    save_obj(result, STX_NFT_TOPMINTED_LAST100_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_fastest_growing_nfts_by_daily_difference_in_mints():
    from etl.provider.stacks.extractors import extract_stacksonchain_raw_data, SOC_FASTEST_GROWING_NFTS_URL
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_NFT_FASTEST_GROWING_PATH
    data = extract_stacksonchain_raw_data(SOC_FASTEST_GROWING_NFTS_URL)
    result = {row['contract']: {'value': row['percentage'], 'contract': row['contract_id'].split('.')[0]} for _, row in data.iterrows()}
    save_obj(result,
             STX_NFT_FASTEST_GROWING_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_most_popular_nft_list():
    from etl.provider.stacks.extractors import extract_stacksonchain_raw_data, SOC_MOST_POPULAR_NFTS_URL
    return extract_stacksonchain_raw_data(SOC_MOST_POPULAR_NFTS_URL)['asset'].tolist()


def run_get_top_nfts_by_number_holders_last24h(asset_list, execution_date):
    from etl.provider.stacks.extractors import extract_stacksonchain_raw_data, SOC_HOLDERS_OF_NFT_URL
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_NFT_TOP_BY_HOLDERS_PATH

    holders = []
    for asset in asset_list:
        try:
            asset_holders = extract_stacksonchain_raw_data(SOC_HOLDERS_OF_NFT_URL.format(asset))
            if not asset_holders.empty:
                asset_holders.set_index('period', inplace=True)
                holders.append((asset.split('.')[1], asset_holders.loc[execution_date, 'holders'].item(), asset.split('.')[0]))
        except (HTTPError, KeyError):
            continue

    holders = sorted(holders, key=lambda x: x[1], reverse=True)[:10]
    logging.info(holders)
    save_obj({x[0]: {'value': x[1], 'contract': x[2]} for x in holders}, STX_NFT_TOP_BY_HOLDERS_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_cheapest_and_most_expensive_nfts(asset_list, execution_date):
    from etl.provider.stacks.extractors import extract_stacksonchain_raw_data, SOC_NFT_FLOOR_PRICE_URL
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_NFT_CHEAPEST_PATH, STX_NFT_MOST_EXPENSIVE_PATH

    floor_prices = []
    for asset in asset_list:
        asset_prices = extract_stacksonchain_raw_data(SOC_NFT_FLOOR_PRICE_URL.format(asset))
        if not asset_prices.empty:
            asset_prices.set_index('Period', inplace=True)
            try:
                floor_price = asset_prices.loc[execution_date, 'Floor'].item()
                if floor_price > 0:
                    floor_prices.append((asset.split('.')[1], floor_price, asset.split('.')[0]))
            except KeyError:
                pass
    sorted_floor_prices = sorted(floor_prices, key=lambda x: x[1], reverse=True)
    most_expensive = sorted_floor_prices[:10]
    cheapest = sorted_floor_prices[-10:]
    save_obj({x[0]: {'value': x[1], 'contract': x[2]} for x in most_expensive}, STX_NFT_MOST_EXPENSIVE_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)
    save_obj({x[0]: x[1] for x in cheapest}, STX_NFT_CHEAPEST_PATH, replace=True,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_latest_whale_movements():
    from etl.provider.stacks.extractors import extract_stx_whale_movements, get_block_times
    from etl.common.loaders import save_df
    from etl.provider.stacks.paths import STX_LATEST_WHALE_MOVEMENTS_PATH
    wms = extract_stx_whale_movements()
    block_times = get_block_times(wms['block_height'].min().item(), wms['block_height'].max().item())
    df_final = pd.merge(wms, block_times, on='block_height')
    df_final['amount'] = df_final['amount'].div(pow(10, 6))
    df_final['tx_id'] = '0x' + df_final['tx_id']
    df_final['tx_type'] = df_final['asset_event_type_id'].apply(_apply_type_id_conversion)
    df_final['explorer_url'] = 'https://explorer.stacks.co/txid/' + df_final['tx_id']
    # df_final['burn_block_time'] = df_final['burn_block_time'].apply(_apply_time_conversion)
    save_df(df_final.drop(columns=['asset_event_type_id']).set_index('tx_id'), STX_LATEST_WHALE_MOVEMENTS_PATH,
            bucket_name=STACKS_DATA_BUCKET_NAME,
            acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_all_pools():
    from etl.provider.stacks.extractors import extract_stacksdata_raw_data
    from etl.provider.stacks.paths import STX_POOLS_PATH, STX_NUMBER_POOLS_PATH
    from etl.common.loaders import save_obj

    payload = {"reportName": "pools"}
    response = extract_stacksdata_raw_data(payload)
    while len(response['total']) == 0:

        response = extract_stacksdata_raw_data(payload)

    df = pd.DataFrame({'pool_address': response['address'],
                       'pool_name': response['address'],
                       'deposits': response['total'],
                       'deposit_count': response['cnt']})
    df['deposits'] = df['deposits'].div(pow(10, 6))
    df.replace({'pool_name': POOL_NAMES}, inplace=True)
    df.sort_values(by=['deposits'], inplace=True, ascending=False, ignore_index=True)
    save_obj({'total_pools': len(df)}, STX_NUMBER_POOLS_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
            acl_policy=DEFAULT_DATAVERSE_POLICY)
    save_obj({row['pool_name']: {'value':round(row['deposits'], SIGNIFICANT_DIGITS), 'address': row['pool_address']}
              for _, row in df.head(10).iterrows()}, STX_POOLS_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
            acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_total_number_of_nfts_now(execution_date):
    from etl.provider.stacks.extractors import extract_stacksdata_raw_data
    from etl.common.loaders import save_obj, get_json
    from etl.provider.stacks.paths import STX_NFT_MINT_BURN_DIFF_PATH, STX_NFT_MINT_BURN_OVER_TIME_PATH

    execution_ts = str(int(_convert_datestring_to_unix_ts(execution_date)))
    endpoint = 'https://stacksonchain.com/api/v1/run/'
    payload_mint = {'query': "select count(*) from nft_events WHERE asset_event_type = 'mint'"}
    payload_burn =  {'query': "select count(*) from nft_events WHERE asset_event_type = 'burn'"}

    data_mints = extract_stacksdata_raw_data(payload_mint, endpoint=endpoint)
    mints = data_mints['count'][0]
    burns = extract_stacksdata_raw_data(payload_burn, endpoint=endpoint)['count'][0]

    active_nfts = mints - burns

    save_obj({'result': active_nfts}, STX_NFT_MINT_BURN_DIFF_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)

    historical_dict = get_json(STX_NFT_MINT_BURN_OVER_TIME_PATH, raw=True, bucket_name=STACKS_DATA_BUCKET_NAME)
    if execution_ts not in historical_dict.keys():
        historical_dict[execution_ts] = active_nfts
    save_obj(historical_dict, STX_NFT_MINT_BURN_OVER_TIME_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_txs_per_block_last100():
    from etl.provider.stacks.extractors import extract_stx_api_general_info, get_number_of_txs_per_block
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_TX_PER_BLOCK_PATH

    current_block = extract_stx_api_general_info()["stacks_tip_height"]
    lookback_block = current_block - 100

    df_blocks = get_number_of_txs_per_block(lookback_block, current_block)
    result = {row['block_height'].item(): row['count'].item() for _, row in df_blocks.iterrows()}
    save_obj(result, STX_TX_PER_BLOCK_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_mktcap_and_vol_last180_days():
    from etl.provider.coingecko.api.coins.extractors import get_daily_market_chart_per_coin
    from etl.common.loaders import save_obj, save_df
    from etl.common.financials import calculate_change, percent_change, calculate_rsi, calculate_rvi, \
        calculate_ema, calculate_nvt
    from etl.provider.stacks.paths import STX_VOLS_PATH, STX_CAPS_PATH, STX_ALL_FINANCIALS_PATH, STX_RSI_PATH,\
        STX_RVI_PATH, STX_EMA_PATH

    data = get_daily_market_chart_per_coin('blockstack')['blockstack']

    vols = data['total_volumes']
    vols = [[t // 1000, v] for [t, v] in vols]
    vols_ordered = sorted(vols, key=lambda x: x[0])
    df_vols = pd.DataFrame({'unix_timestamp': [x[0] for x in vols_ordered], 'vol_usd': [x[1] for x in vols_ordered]})
    df_vols['time_string'] = df_vols['unix_timestamp'].apply(_apply_time_conversion)
    vols_final = {row['unix_timestamp']: round(row['vol_usd'], SIGNIFICANT_DIGITS) for _, row in df_vols.iterrows()}

    caps = data['market_caps']
    caps = [[t // 1000, c] for [t, c] in caps]
    caps_ordered = sorted(caps, key=lambda x: x[0])
    df_caps = pd.DataFrame({'unix_timestamp': [x[0] for x in caps_ordered], 'mktcap_usd': [x[1] for x in caps_ordered]})
    df_caps['time_string'] = df_caps['unix_timestamp'].apply(_apply_time_conversion)
    caps_final = {row['unix_timestamp']: round(row['mktcap_usd'], SIGNIFICANT_DIGITS) for _, row in df_caps.iterrows()}

    save_obj(vols_final, STX_VOLS_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)
    save_obj(caps_final, STX_CAPS_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)

    prices = data['prices']
    prices = [[t // 1000, p] for [t, p] in prices]
    prices_ordered = sorted(prices, key=lambda x: x[0])
    df_prices = pd.DataFrame({'unix_timestamp': [x[0] for x in prices_ordered],
                              'price_usd': [round(x[1],SIGNIFICANT_DIGITS) for x in prices_ordered]})
    df_prices['time_string'] = df_caps['unix_timestamp'].apply(_apply_time_conversion)

    df_partial = pd.merge(df_vols, df_caps, on='time_string')
    df_cgdata = pd.merge(df_partial, df_prices, on='time_string')

    df_final = (df_cgdata.pipe(percent_change).pipe(calculate_rsi).pipe(calculate_rvi).pipe(calculate_change)
                .pipe(calculate_ema).pipe(calculate_nvt))
    df_final.fillna(0, inplace=True)
    save_df(df_final, STX_ALL_FINANCIALS_PATH, bucket_name=STACKS_DATA_BUCKET_NAME,
             acl_policy=DEFAULT_DATAVERSE_POLICY)

    df_rsi = df_final[df_final['rsi'] != 0][['unix_timestamp_x', 'rsi']]
    save_obj({str(int(row['unix_timestamp_x'])): row['rsi'] for _, row in df_rsi.iterrows()}, STX_RSI_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True, acl_policy=DEFAULT_DATAVERSE_POLICY)

    df_rvi = df_final[df_final['rvi'] != 0][['unix_timestamp_x', 'rvi']]
    save_obj({str(int(row['unix_timestamp_x'])): row['rvi'] for _, row in df_rvi.iterrows()}, STX_RVI_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True, acl_policy=DEFAULT_DATAVERSE_POLICY)

    df_ema = df_final[df_final['7dayEMA'] != 0][['unix_timestamp_x', '7dayEMA']]
    save_obj({str(int(row['unix_timestamp_x'])): row['7dayEMA'] for _, row in df_ema.iterrows()}, STX_EMA_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True, acl_policy=DEFAULT_DATAVERSE_POLICY)

def run_get_lunar_metrics():
    from etl.provider.lunarcrush.extractors import get_spot_lunar_metrics, get_historical_lunar_metrics_past30days
    from etl.common.loaders import save_obj
    from etl.provider.stacks.paths import STX_LUNAR_SPOT_PATH, STX_LUNAR_HISTORICAL_PATH

    save_obj(get_spot_lunar_metrics(), STX_LUNAR_SPOT_PATH, bucket_name=STACKS_DATA_BUCKET_NAME, replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)
    save_obj(get_historical_lunar_metrics_past30days(), STX_LUNAR_HISTORICAL_PATH, bucket_name=STACKS_DATA_BUCKET_NAME,
             replace=True,
             acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_sanity_check():
    from etl.provider.stacks.paths import STX_SANITY_CHECK_PATH
    from etl.common.loaders import save_obj, get_json
    from etl.provider.stacks.extractors import extract_latest_block_no

    old_max = get_json(STX_SANITY_CHECK_PATH, raw=True, bucket_name=STACKS_DATA_BUCKET_NAME)['maxblock']

    new_max = extract_latest_block_no()

    if new_max > old_max:
        save_obj({'maxblock': new_max}, STX_SANITY_CHECK_PATH, bucket_name=STACKS_DATA_BUCKET_NAME,
             replace=True)
    else:
        raise ValueError('db sync did not progress')


def run_get_mempool_over_time():
    from etl.provider.stacks.extractors import extract_stacksdata_raw_data
    from etl.common.loaders import save_obj
    from etl.common.helpers import get_every_nth_index
    from etl.provider.stacks.paths import STX_MEMPOOL_PATH

    payload = {"reportName": "mempool_tx"}
    data = extract_stacksdata_raw_data(payload, endpoint='https://api.stacksdata.info/v1/ts')
    keys = data['ts']
    values = data['value']
    indices = get_every_nth_index(keys, 3)
    save_obj({keys[i]//1000: values[i] for i in indices}, STX_MEMPOOL_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME, replace=True, acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_microblocks_per_day():
    from etl.provider.stacks.extractors import get_microblock_times
    from etl.common.loaders import save_obj
    from collections import Counter
    from etl.provider.stacks.paths import STX_MICROBLOCKS_OVER_TIME_PATH

    microblock_times = get_microblock_times().dt.to_pydatetime()
    microblock_times = [_to_timestamp(datetime.datetime.combine(v.date(), datetime.time.min)) for v in microblock_times]
    save_obj(dict(Counter(microblock_times)), STX_MICROBLOCKS_OVER_TIME_PATH, bucket_name=STACKS_DATA_BUCKET_NAME,
             replace=True, acl_policy=DEFAULT_DATAVERSE_POLICY)


def run_get_nonzero_balances_over_time(execution_date):
    from etl.common.loaders import save_obj, get_json
    from etl.provider.stacks.paths import STX_NONZERO_BALS_OVER_TIME_PATH, STX_RAW_WALLET_BALANCES_PATH

    rolling_date = datetime.date(2022, 2, 28)
    end_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d').date()

    result = {}

    while rolling_date <= end_date:
        date_string = rolling_date.strftime('%Y-%m-%d')
        rolling_ts = int(datetime.datetime.combine(rolling_date, datetime.datetime.min.time()).timestamp())
        # if rolling_ts >= end_ts:
        #     break

        logging.info('Processing '+ date_string)
        try:
            nonzero = get_json(STX_RAW_WALLET_BALANCES_PATH.format(execution_date=date_string), raw=True,
                                        bucket_name=STACKS_DATA_BUCKET_NAME)['nonzero_bals']
            result[rolling_ts] = nonzero
        except ClientError:
            logging.info('cannot find entry at '+ date_string)
            pass

        rolling_date = rolling_date + datetime.timedelta(days=1)

    save_obj(result, STX_NONZERO_BALS_OVER_TIME_PATH,
             bucket_name=STACKS_DATA_BUCKET_NAME,
             replace=True, acl_policy=DEFAULT_DATAVERSE_POLICY)

