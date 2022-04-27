from etl.common.extractors import rest_get_express
from airflow.models import Variable


def get_cryptocurrency_to_usd_conversion_at_ts(coin, utc_ts=None):
    """
    Gets the conversion rate between a given cryptocurrency and USD at the given Unix ts.
    :param coin:
    str - the crypto token name eg. BTC, ETH
    :param utc_ts:
    int - the Unix epoch ts at which to fetch the rate
    :return:
    float - the conversion rate to USD
    """
    data = rest_get_express('https://min-api.cryptocompare.com/data/pricehistorical?fsym={0}&tsyms=USD&ts={1}'
                            '&api_key={2}'.format(coin, str(utc_ts), Variable.get('CRYPTOCOMPARE_API_KEY')))
    return data[coin]['USD']
