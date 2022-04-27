from etl.provider.coingecko.api.common.extractors import COIN_GECKO_API_ROOT, get_resource_instance_info

COIN_GECKO_API_COINS = f"{COIN_GECKO_API_ROOT}coins/"


def _get_coin_info(url_template, resource_ids, skip_if_not_found=True):
    base_url = COIN_GECKO_API_COINS
    return get_resource_instance_info(base_url, url_template, resource_ids, skip_if_not_found=skip_if_not_found)


def get_hourly_market_chart_per_coin(coin_id):
    template = '{instance_id}/market_chart?vs_currency=usd&days=90&interval=hourly'
    return _get_coin_info(template, [coin_id], skip_if_not_found=False)


def get_daily_market_chart_per_coin(coin_id):
    template = '{instance_id}/market_chart?vs_currency=usd&days=180&interval=daily'
    return _get_coin_info(template, [coin_id], skip_if_not_found=False)


def get_coin_details_with_spot_market(exchange_ids):
    template = '{instance_id}?localization=false&tickers=false&market_data=true&' \
               'community_data=false&developer_data=false&sparkline=false'
    return _get_coin_info(template, exchange_ids)

