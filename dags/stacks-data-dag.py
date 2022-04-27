from __future__ import print_function
from etl.common.slack_integration import task_fail_slack_alert
from etl.provider.stacks.paths import *

from datetime import datetime

from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 12, 15),
    'provide_context': True,
    'retries': 2
}

dag = DAG('stacks-data-dag-milestone2', schedule_interval='@daily',
          default_args=args, tags=['stacks'],
          on_failure_callback=task_fail_slack_alert)


def _pull_addresses_list(kwargs):
    ti = kwargs['ti']
    return ti.xcom_pull(task_ids='daily_addx_growth_over_time')


def _pull_whale_balance_data(kwargs):
    ti = kwargs['ti']
    return ti.xcom_pull(task_ids='dist_wallet_balances')


def _pull_popular_nft_list(kwargs):
    ti = kwargs['ti']
    return ti.xcom_pull(task_ids='most_popular_nft_list')


def get_daily_transactions_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_daily_transactions_over_time
    run_get_daily_transactions_over_time(kwargs['ds'])


def get_daily_address_growth_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_daily_address_growth_over_time
    return run_get_daily_address_growth_over_time(kwargs['ds'])


def get_daily_active_addresses_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_daily_active_addresses_over_time
    run_get_daily_active_addresses_over_time(kwargs['ds'])


def get_historical_price_data_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_historical_price_data_over_time
    run_get_historical_price_data_over_time()


def get_stx_stacked_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_stx_stacked_over_time
    run_get_stx_stacked_over_time(kwargs['ds'])


def get_distribution_of_wallet_balances_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_distribution_of_wallet_balances
    addresses_list = _pull_addresses_list(kwargs)
    return run_distribution_of_wallet_balances(kwargs['ds'], addresses_list)


def copy_dt_data_to_latest_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_copy_stacks_dataset_to_latest
    run_copy_stacks_dataset_to_latest(kwargs['ds'], STX_DAILY_TXS_OVER_TIME_PATH)


def copy_agbd_data_to_latest_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_copy_stacks_dataset_to_latest
    run_copy_stacks_dataset_to_latest(kwargs['ds'], STX_ADDRESSES_GROWTH_OVER_TIME_PATH)
    run_copy_stacks_dataset_to_latest(kwargs['ds'], STX_WALLET_BALANCES_PATH)
    run_copy_stacks_dataset_to_latest(kwargs['ds'], STX_RAW_WALLET_BALANCES_PATH)


def copy_daa_data_to_latest_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_copy_stacks_dataset_to_latest
    run_copy_stacks_dataset_to_latest(kwargs['ds'], STX_ADDRESSES_DAILY_ACTIVE_OVER_TIME_PATH)


# def copy_hp_data_to_latest_wrapper(**kwargs):
#     from etl.provider.stacks.runners import run_copy_stacks_dataset_to_latest
#     run_copy_stacks_dataset_to_latest(kwargs['ds'], ST


def copy_stxed_data_to_latest_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_copy_stacks_dataset_to_latest
    run_copy_stacks_dataset_to_latest(kwargs['ds'], STX_STACKED_OVER_TIME_PATH)


def extract_latest_time_interval_data_wrapper(**kwargs):
    from etl.provider.stacks.runners import extract_stx_price_over_time_intervals
    extract_stx_price_over_time_intervals(kwargs['ds'])


def get_fastest_growing_nfts_by_daily_difference_in_mints_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_fastest_growing_nfts_by_daily_difference_in_mints
    run_get_fastest_growing_nfts_by_daily_difference_in_mints()


def get_latest_whale_movements_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_latest_whale_movements
    run_get_latest_whale_movements()


def get_all_pool_data_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_all_pools
    run_get_all_pools()


def get_mktcap_and_vol_last180days_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_mktcap_and_vol_last180_days
    run_get_mktcap_and_vol_last180_days()


def get_lunar_metrics_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_lunar_metrics
    run_get_lunar_metrics()


def sanity_check_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_sanity_check
    run_sanity_check()


def get_microblocks_per_day_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_microblocks_per_day
    run_get_microblocks_per_day()


def get_nonzero_balances_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_nonzero_balances_over_time
    run_get_nonzero_balances_over_time(kwargs['ds'])


def copy_whale_balances_to_db(**kwargs):
    from etl.provider.stacks.runners import run_save_whale_balances_history
    records = _pull_whale_balance_data(kwargs)
    run_save_whale_balances_history(records)


def get_most_popular_nft_list_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_most_popular_nft_list
    return run_get_most_popular_nft_list()


def get_top_nfts_by_number_holders_last24h_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_top_nfts_by_number_holders_last24h
    asset_list = _pull_popular_nft_list(kwargs)
    run_get_top_nfts_by_number_holders_last24h(asset_list, kwargs['ds'])


def get_cheapest_and_most_expensive_nfts_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_cheapest_and_most_expensive_nfts
    asset_list = _pull_popular_nft_list(kwargs)
    run_get_cheapest_and_most_expensive_nfts(asset_list, kwargs['ds'])


# dummy = DummyOperator(task_id='dummy', dag=dag)
latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

get_daily_transactions_over_time = PythonOperator(
    task_id='daily_txs_over_time',
    dag=dag,
    python_callable=get_daily_transactions_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_daily_address_growth_over_time = PythonOperator(
    task_id='daily_addx_growth_over_time',
    dag=dag,
    python_callable=get_daily_address_growth_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_daily_active_addresses_over_time = PythonOperator(
    task_id='daily_addx_active_over_time',
    dag=dag,
    python_callable=get_daily_active_addresses_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_historical_price_data_over_time = PythonOperator(
    task_id='price_over_time',
    dag=dag,
    python_callable=get_historical_price_data_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_distribution_of_wallet_balances = PythonOperator(
    task_id='dist_wallet_balances',
    dag=dag,
    python_callable=get_distribution_of_wallet_balances_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

save_whale_balances_to_db = PythonOperator(
    task_id='save_whale_balances_to_db',
    dag=dag,
    python_callable=copy_whale_balances_to_db,
    provide_context=True,
    pool='stacks-pool'
)

get_stx_stacked_over_time = PythonOperator(
    task_id='get_stx_stacked_over_time',
    dag=dag,
    python_callable=get_stx_stacked_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

copy_dt_data_to_latest = PythonOperator(
    task_id='copy_dt_data_to_latest',
    dag=dag,
    python_callable=copy_dt_data_to_latest_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

copy_agbd_data_to_latest = PythonOperator(
    task_id='copy_agbd_data_to_latest',
    dag=dag,
    python_callable=copy_agbd_data_to_latest_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

copy_daa_data_to_latest = PythonOperator(
    task_id='copy_daa_data_to_latest',
    dag=dag,
    python_callable=copy_daa_data_to_latest_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

# copy_hp_data_to_latest = PythonOperator(
#     task_id='copy_hp_data_to_latest',
#     dag=dag,
#     python_callable=copy_hp_data_to_latest_wrapper,
#     provide_context=True,
#     pool='stacks-pool'
# )

copy_stxed_data_to_latest = PythonOperator(
    task_id='copy_stxed_data_to_latest',
    dag=dag,
    python_callable=copy_stxed_data_to_latest_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

extract_latest_time_interval_data = PythonOperator(
    task_id='extract_latest_time_interval_data',
    dag=dag,
    python_callable=extract_latest_time_interval_data_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_fastest_growing_nfts_by_daily_difference_in_mints = PythonOperator(
    task_id='fastest_growing_nfts_by_daily_difference_in_mints',
    dag=dag,
    python_callable=get_fastest_growing_nfts_by_daily_difference_in_mints_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_latest_whale_movements = PythonOperator(
    task_id='latest_whale_movements',
    dag=dag,
    python_callable=get_latest_whale_movements_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_all_pool_data = PythonOperator(
    task_id='all_pool_data',
    dag=dag,
    python_callable=get_all_pool_data_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_mktcap_and_vol_last180days = PythonOperator(
    task_id='mktcap_and_vol_last180',
    dag=dag,
    python_callable=get_mktcap_and_vol_last180days_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_lunar_metrics = PythonOperator(
    task_id='get_lunar_metrics',
    dag=dag,
    python_callable=get_lunar_metrics_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

sanity_check = PythonOperator(
    task_id='sanity_check',
    dag=dag,
    python_callable=sanity_check_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_microblocks_per_day = PythonOperator(
    task_id='microblocks_per_day',
    dag=dag,
    python_callable=get_microblocks_per_day_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_nonzero_balances_over_time = PythonOperator(
    task_id='nonzero_bals_over_time',
    dag=dag,
    python_callable=get_nonzero_balances_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_most_popular_nft_list = PythonOperator(
    task_id='most_popular_nft_list',
    dag=dag,
    python_callable=get_most_popular_nft_list_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_top_nfts_by_number_holders_last24h = PythonOperator(
    task_id='top_nfts_by_number_holders_last24h',
    dag=dag,
    python_callable=get_top_nfts_by_number_holders_last24h_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_cheapest_and_most_expensive_nfts = PythonOperator(
    task_id='cheapest_and_most_expensive_nfts',
    dag=dag,
    python_callable=get_cheapest_and_most_expensive_nfts_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_daily_transactions_over_time >> copy_dt_data_to_latest
get_daily_address_growth_over_time >> get_distribution_of_wallet_balances >> copy_agbd_data_to_latest
get_distribution_of_wallet_balances >> get_nonzero_balances_over_time
get_distribution_of_wallet_balances >> save_whale_balances_to_db
get_daily_active_addresses_over_time >> copy_daa_data_to_latest
get_historical_price_data_over_time >> extract_latest_time_interval_data # >> calculate_rsi_rvi_ema
get_stx_stacked_over_time >> copy_stxed_data_to_latest
get_most_popular_nft_list >> get_top_nfts_by_number_holders_last24h
get_most_popular_nft_list >> get_cheapest_and_most_expensive_nfts


latest_only >> copy_dt_data_to_latest
latest_only >> copy_agbd_data_to_latest
latest_only >> copy_daa_data_to_latest
latest_only >> copy_stxed_data_to_latest
latest_only >> extract_latest_time_interval_data

latest_only >> get_fastest_growing_nfts_by_daily_difference_in_mints
latest_only >> get_latest_whale_movements
latest_only >> get_all_pool_data

latest_only >> get_mktcap_and_vol_last180days

latest_only >> get_lunar_metrics

latest_only >> get_microblocks_per_day

latest_only >> sanity_check

# latest_only >> get_nonzero_balances_over_time
