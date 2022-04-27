from __future__ import print_function
from etl.common.slack_integration import task_fail_slack_alert

from datetime import datetime

from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.hooks.ssh_hook import SSHHook

args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 12, 16, 7, 0, 0),
    'provide_context': True,
    'retries': 2
}

dag = DAG('stacks-data-dag-halfhourly', schedule_interval='*/30 * * * *',
          default_args=args, tags=['stacks', 'real-time'],
          on_failure_callback=task_fail_slack_alert)


def get_spot_stats_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_create_stx_spot_stats
    run_create_stx_spot_stats(kwargs['ds'])


def get_mining_data_last100_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_mining_data_last100
    run_get_mining_data_last100()


def get_mining_risk_reward_comparison_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_calculate_mining_risk_reward_comparison
    run_calculate_mining_risk_reward_comparison()


def get_most_successful_miners_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_most_successful_miners
    run_get_most_successful_miners()


def get_stacking_reward_per_block_last100_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_stacking_reward_per_block_last100
    run_get_stacking_reward_per_block_last100()


def get_nft_trade_volume_stx_per_block_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_nft_trade_volume_stx_per_block
    run_get_nft_trade_volume_stx_per_block()


def get_top_nfts_by_trade_vol_last100_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_top_nfts_by_trade_vol_last100
    run_get_top_nfts_by_trade_vol_last100()


def get_top_nfts_by_mints_last100_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_top_nfts_by_mints_last100
    run_get_top_nfts_by_mints_last100()


def get_total_number_of_nfts_now_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_total_number_of_nfts_now
    run_get_total_number_of_nfts_now(kwargs['ds'])


def get_txs_per_block_last100_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_txs_per_block_last100
    run_get_txs_per_block_last100()


def get_mempool_over_time_wrapper(**kwargs):
    from etl.provider.stacks.runners import run_get_mempool_over_time
    run_get_mempool_over_time()


# dummy = DummyOperator(task_id='dummy', dag=dag)
latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

create_stx_spot_stats = PythonOperator(
    task_id='create_stx_spot_stats',
    dag=dag,
    python_callable=get_spot_stats_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_mining_data_last100 = PythonOperator(
    task_id='mining_data_last100',
    dag=dag,
    python_callable=get_mining_data_last100_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_mining_risk_reward_comparison = PythonOperator(
    task_id='mining_risk_reward_comparison',
    dag=dag,
    python_callable=get_mining_risk_reward_comparison_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_most_successful_miners = PythonOperator(
    task_id='most_successful_miners',
    dag=dag,
    python_callable=get_most_successful_miners_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_stacking_reward_per_block_last100 = PythonOperator(
    task_id='stacking_reward_per_block_last100',
    dag=dag,
    python_callable=get_stacking_reward_per_block_last100_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_nft_trade_volume_stx_per_block = PythonOperator(
    task_id='nft_trade_volume_stx_per_block',
    dag=dag,
    python_callable=get_nft_trade_volume_stx_per_block_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_top_nfts_by_trade_vol_last100 = PythonOperator(
    task_id='top_nfts_by_trade_vol_last100',
    dag=dag,
    python_callable=get_top_nfts_by_trade_vol_last100_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_top_nfts_by_mints_last100 = PythonOperator(
    task_id='top_nfts_by_mints_last100',
    dag=dag,
    python_callable=get_top_nfts_by_mints_last100_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_total_number_of_nfts_now = PythonOperator(
    task_id='total_number_of_nfts_now',
    dag=dag,
    python_callable=get_total_number_of_nfts_now_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_txs_per_block_last100 = PythonOperator(
    task_id='txs_per_block_last200',
    dag=dag,
    python_callable=get_txs_per_block_last100_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

get_mempool_over_time = PythonOperator(
    task_id='get_microblocks_over_time',
    dag=dag,
    python_callable=get_mempool_over_time_wrapper,
    provide_context=True,
    pool='stacks-pool'
)

invalidate_cloudfront_cache = SSHOperator(
    task_id="invalidate_cloudfront_cache",
    command='aws cloudfront create-invalidation --distribution-id YOUR_CLOUDFRONT_DIST_ID --paths "/*"',
    ssh_hook=SSHHook('ssh_default'),
    dag=dag
)

latest_only >> create_stx_spot_stats >> invalidate_cloudfront_cache
latest_only >> get_mining_data_last100 >> get_mining_risk_reward_comparison >> get_most_successful_miners >> invalidate_cloudfront_cache

latest_only >> get_stacking_reward_per_block_last100 >> invalidate_cloudfront_cache

latest_only >> get_nft_trade_volume_stx_per_block >> invalidate_cloudfront_cache
latest_only >> get_top_nfts_by_trade_vol_last100 >> invalidate_cloudfront_cache
latest_only >> get_top_nfts_by_mints_last100 >> invalidate_cloudfront_cache
latest_only >> get_total_number_of_nfts_now >> invalidate_cloudfront_cache
latest_only >> get_txs_per_block_last100 >> invalidate_cloudfront_cache
latest_only >> get_mempool_over_time >> invalidate_cloudfront_cache
