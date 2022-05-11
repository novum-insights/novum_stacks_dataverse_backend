import logging
import requests
import statistics
from pandas import read_sql, DataFrame
from lxml import etree
from urllib.request import urlopen
from gql import gql, Client, RequestsHTTPTransport
from etl.common.loaders import get_stacks_db_hook, get_warehouse_db_hook
from etl.common.extractors import rest_get_express


_TIMEOUT = 60

_CLIENT_STACKSAPI = None


CURRENT_CYCLE_URL_STACKINGCLUB = 'https://stacking.club/cycles/current'
STX_MINING_GRAPH_URL = 'https://mining-importer.up.railway.app/v1/graphql'
STX_STACKING_CYCLE_DETAILS_ENDPOINT = 'https://api.stacking.club/api/cycle-info?cycle={}'
STX_STACKING_REWARDS_PER_CYCLE_URL = 'https://api.stacking-club.com/api/block-rewards?cycle={}'
STXMINING_BLOCK_INFO_URL = 'https://api.stxmining.club/api/v2/getBlockInfoByNumber?block_number={}'
STXMINING_OVERVIEW_DATA_URL = 'https://api.stxmining.club/api/v2/getOverviewData'
SOC_NFT_TRADE_VOLUME_URL = 'https://stacksonchain.com/api/nftvolume'
SOC_TOPTRADED_VOL_LAST100_URL = 'https://stacksonchain.com/api/nfttraded/100'
SOC_TOPMINTS_LAST100_URL = 'https://stacksonchain.com/api/nftmints/100'
SOC_FASTEST_GROWING_NFTS_URL = 'https://stacksonchain.com/api/nftgrowth'
SOC_MOST_POPULAR_NFTS_URL = 'https://stacksonchain.com/api/popularnfts'
SOC_HOLDERS_OF_NFT_URL = 'https://stacksonchain.com/api/nftholders/{}?period=1d'
SOC_NFT_FLOOR_PRICE_URL = 'https://stacksonchain.com/api/nftfloor/{}?period=1d'


STACKS_API_BASE_ENDPOINT = 'http://your-stacks-api-instance-url-here'
STACKS_API_BALANCE_ENDPOINT = STACKS_API_BASE_ENDPOINT + '/extended/v1/address/{}/balances'
STACKS_API_INFO_ENDPOINT = STACKS_API_BASE_ENDPOINT + '/v2/info'
STACKS_API_FEES_ENDPOINT = STACKS_API_BASE_ENDPOINT + '/v2/fees/transfer'
STACKS_API_SUPPLY_ENDPOINT = STACKS_API_BASE_ENDPOINT + '/extended/v1/stx_supply/circulating/plain'

APY_XPATH = '//*[@id="__next"]/div/div[2]/div[1]/div/div/div[2]/div[2]/div[1]/div[2]/div'


TOTAL_COMMITTED_PER_BLOCK_LAST100_QUERY = gql("""
query getBtcCommitsPerBlock {
    block_info(order_by: {stacks_block_height: desc}, offset: 0, limit: 100) {
            stacks_block_height
            totalSpent: winner_to_all_commit_aggregate {
                        aggregate {
                            sum {
                                    commit_value
                                }
                        }
            }
    }
}
""")

BLOCK_MINING_INFO_QUERY = gql("""
query GetBlockInfo($stacks_block_height: Int!) {
    block_info(where: {stacks_block_height: {_eq: $stacks_block_height}}) {
            block_reward
            tx_reward
            stacks_block_height
            winner_stx_address
            blockCommits: winner_to_all_commit {
                    is_winner
                    commit_value
                    stx_address
                    commit_btc_tx_id
            }
        }
    }
""")

TOTAL_BTC_COMMITTED_ALLTIME_QUERY = gql("""
query getGeneralStats {
    btcSpentAllTime: commit_info_aggregate {
        aggregate {
            sum {
                commit_value
            }
        }
    }
    btcFeesAllTime: commit_gas_info_aggregate {
        aggregate {
            sum {
                commit_btc_gas_fee
            }
        }
    }
    
}
""")

def _determine_stacking_cycle_phase(current, start, end):
    if start <= current <= start + 100:
        return 'Prepare'
    elif end - 90 <= current <= end:
        return 'Burn'
    else:
        return 'PoX'


def extract_txs_data_between_timestamps(low_ts, high_ts):
    sql = "SELECT COUNT(*) AS ct FROM txs WHERE burn_block_time >= %(start)s AND burn_block_time < %(end)s"
    data = read_sql(sql, get_stacks_db_hook().get_conn(), params= {'start': low_ts, 'end': high_ts})
    return int(data.iloc[0]['ct'])


def extract_latest_block_no():
    sql = "SELECT MAX(block_height) AS blk FROM txs"
    data = read_sql(sql, get_stacks_db_hook().get_conn())
    return int(data.iloc[0]['blk'])


def extract_block_heights_between_timestamps(low_ts, high_ts):
    sql = "SELECT block_height FROM blocks WHERE burn_block_time >= %(start)s AND burn_block_time < %(end)s"
    data = read_sql(sql, get_stacks_db_hook().get_conn(), params={'start': low_ts, 'end': high_ts})
    return list(data['block_height'])


def extract_stx_event_parties_between_blocks(low_block, high_block):
    sql = "SELECT sender, recipient FROM stx_events WHERE block_height >= %(low)s AND block_height <= %(high)s"
    data = read_sql(sql, get_stacks_db_hook().get_conn(), params={'low': low_block, 'high': high_block})
    return data


def extract_stx_price_from_ts(low_ts):
    sql = "SELECT * FROM stx_historical_prices WHERE unix_timestamp >= %(start)s"
    data = read_sql(sql, get_warehouse_db_hook().get_conn(), params={'start': low_ts})
    return data


def delete_existing_whale_balances_at_timestamp(ts):
    sql = "DELETE FROM whale_historical_stx_balances WHERE timestamp = %s"
    get_warehouse_db_hook().get_conn().cursor().execute(sql, (ts,))


def extract_latest_stx_whale_movement():
    sql = "SELECT burn_block_time, token_transfer_amount " \
          "FROM txs " \
          "WHERE token_transfer_amount >= 1000000000000 ORDER BY block_height DESC, token_transfer_amount DESC " \
          "FETCH FIRST 1 ROW ONLY"
    data = read_sql(sql, get_stacks_db_hook().get_conn())
    return data.iloc[0]


# remember to add 0x before the tx_id returned here
def extract_stx_whale_movements():
    sql = "SELECT encode(tx_id, 'hex') AS tx_id, block_height, asset_event_type_id, amount, sender, recipient " \
          "FROM stx_events " \
          "WHERE amount >= 1000000000000 AND sender IS NOT NULL AND recipient IS NOT NULL ORDER BY block_height DESC, amount DESC " \
          "FETCH FIRST 500 ROWS ONLY"
    data = read_sql(sql, get_stacks_db_hook().get_conn())
    return data


def get_block_times(start_block, end_block):
    sql = "SELECT block_height, burn_block_time FROM blocks " \
          "WHERE block_height >= %(start)s  AND block_height <= %(end)s AND canonical = TRUE"
    data = read_sql(sql, get_stacks_db_hook().get_conn(), params={'start': start_block, 'end': end_block})
    return data


def get_btc_block_times(start_block, end_block):
    sql = "SELECT burn_block_height AS block_height, burn_block_time FROM blocks " \
          "WHERE burn_block_height >= %(start)s  AND burn_block_height <= %(end)s AND canonical = TRUE"
    data = read_sql(sql, get_stacks_db_hook().get_conn(), params={'start': start_block, 'end': end_block})
    return data


def get_number_of_txs_per_block(start_block, end_block):
    sql = "SELECT count(*), block_height FROM txs WHERE block_height >= %(start)s AND block_height <= %(end)s " \
          "GROUP BY block_height " \
          "ORDER BY block_height DESC"
    data = read_sql(sql, get_stacks_db_hook().get_conn(), params={'start': start_block, 'end': end_block})
    return data


def get_microblock_times():
    sql = "SELECT receive_timestamp FROM microblocks"
    data = read_sql(sql, get_stacks_db_hook().get_conn())
    return data['receive_timestamp']


def extract_latest_transfer_fee():
    sql = "SELECT fee_rate FROM txs WHERE fee_rate IS NOT NULL AND token_transfer_recipient_address IS NOT NULL " \
          "ORDER BY block_height DESC FETCH FIRST 10 ROWS ONLY"
    data = read_sql(sql, get_stacks_db_hook().get_conn())
    return statistics.median([row['fee_rate'].item() for _, row in data.iterrows()])


def extract_balance_from_single_address(stx_address):
    r = requests.get(STACKS_API_BALANCE_ENDPOINT.format(stx_address))
    if r.status_code == 200:
        return int(r.json()['stx']['balance'])*pow(10, -6)
    else:
        return None


def extract_stx_reward_cycle_details(cycle_no):
    return rest_get_express(STX_STACKING_CYCLE_DETAILS_ENDPOINT.format(cycle_no))


def extract_stx_reward_details_per_cycle(cycle_no):
    return rest_get_express(STX_STACKING_REWARDS_PER_CYCLE_URL.format(cycle_no))


def extract_stx_api_general_info():
    return rest_get_express(STACKS_API_INFO_ENDPOINT)


def determine_current_stx_reward_cycle():
    current_btc_height = extract_stx_api_general_info()['burn_block_height']
    for i in range(14, 500):
        stx_cycle_info = extract_stx_reward_cycle_details(i)
        if stx_cycle_info['cycleStartHeight'] <= current_btc_height < stx_cycle_info['cycleEndHeight']:
            logging.info('Current STX mining cycle is number ' + str(i))
            stx_cycle_info['blocks_left'] = stx_cycle_info['cycleEndHeight'] - stx_cycle_info['currentHeight']
            stx_cycle_info['current_phase'] = _determine_stacking_cycle_phase(stx_cycle_info['currentHeight'],
                                                                              stx_cycle_info['cycleStartHeight'],
                                                                              stx_cycle_info['cycleEndHeight'])
            return i, stx_cycle_info
    return None, None


def extract_mining_info_per_block_number(block_no):
    result = _get_data(BLOCK_MINING_INFO_QUERY, _get_client(), {'stacks_block_height': block_no})['block_info'][0]
    total_burn_fee = sum([d['commit_value'] for d in result['blockCommits']])
    result['total_burn_fee'] = total_burn_fee
    return result


def extract_mining_overview_data():
    return _get_data(TOTAL_BTC_COMMITTED_ALLTIME_QUERY, _get_client())


def extract_nft_total_trade_volume_stx_per_block():
    return rest_get_express(SOC_NFT_TRADE_VOLUME_URL)


def extract_stacksonchain_raw_data(endpoint):
    r = rest_get_express(endpoint)
    df = DataFrame(r['columns'])
    if 'redirect' in r.keys():
        df['redirect'] = r['redirect']
    return df


def extract_stacksdata_raw_data(payload, endpoint='https://api.stacksdata.info/v1/run'):
    # https://stacksonchain.com/api/v1/run/ alt
    r = requests.post(endpoint, json=payload)
    return r.json()['columns']


def get_fastfee_in_mstx():
    r = requests.get(STACKS_API_FEES_ENDPOINT)
    return int(r.text)


def get_circulating_supply():
    r = requests.get(STACKS_API_SUPPLY_ENDPOINT)
    return float(r.text)


def _get_data(query, client, params=None):
    params = params.copy() if params else {}
    result = client.execute(query, variable_values=params)
    return result


def _get_client():
    global _CLIENT_STACKSAPI
    if _CLIENT_STACKSAPI is None:
        _CLIENT_STACKSAPI = _create_client(url=STX_MINING_GRAPH_URL)
    return _CLIENT_STACKSAPI


def _create_client(url):
    transport = RequestsHTTPTransport(url=url, headers={'x-hasura-admin-secret': 'DaemonTechnologies123@'})
    return Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=_TIMEOUT)


def extract_apy():
    response = urlopen(CURRENT_CYCLE_URL_STACKINGCLUB)
    htmlparser = etree.HTMLParser()
    tree = etree.parse(response, htmlparser)
    try:
        apy = tree.xpath(APY_XPATH)[0].text
    except IndexError:
        apy = '-.-%'
    return apy