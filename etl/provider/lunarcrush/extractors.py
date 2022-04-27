from etl.common.extractors import rest_get_express
from airflow.models import Variable


LUNARCRUSH_API_KEY = Variable.get('LUNARCRUSH_API_KEY')
LUNARCRUSH_COIN_DETAILS_URL = 'https://api.lunarcrush.com/v2?data=assets&key={}&symbol={}'


def get_spot_lunar_metrics(coin='STX'):
    data = rest_get_express(LUNARCRUSH_COIN_DETAILS_URL.format(LUNARCRUSH_API_KEY, coin))['data'][0]
    return {'galaxy': data['galaxy_score'], 'alt_rank': data['alt_rank'], 'alt_rank_30d': data['alt_rank_30d']}


def get_historical_lunar_metrics_past30days(coin='STX'):
    data = rest_get_express(LUNARCRUSH_COIN_DETAILS_URL.format(LUNARCRUSH_API_KEY, coin))['data'][0]
    return {d['time']: {'galaxy': d['galaxy_score'], 'alt_rank': d['alt_rank']} for d in data['timeSeries']}


def get_spot_price_for_asset(asset_symbol):
    data = rest_get_express(LUNARCRUSH_COIN_DETAILS_URL.format(LUNARCRUSH_API_KEY, asset_symbol))
    return data['data']['price']
