
from datetime import datetime

from airflow.models import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

### Update these ids to match your account ###
DBT_CLOUD_CONN_ID = "dbt_cloud"
ACCOUNT_ID = "70471823417825"
JOB_ID = "70471823419992"



print("============================================")
print(f"CONNECTION_NAME {DBT_CLOUD_CONN_ID}")
print(f"ACCOUNT_ID {ACCOUNT_ID}")
print(f"JOB_ID {JOB_ID}")
print("============================================")

with DAG(
    # usar conexão criada no airflow
    dag_id="dbt_cloud_run_job",
    default_args={"dbt_cloud_conn_id": DBT_CLOUD_CONN_ID, "account_id": ACCOUNT_ID},
    start_date=datetime(2021, 1, 1),
    # rodar às 6h diariamente
    schedule_interval='0 6 * * *',  
    catchup=False,
) as dag:

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=JOB_ID,
        check_interval=10,
        timeout=300,
        retry_from_failure=True,
    )

    trigger_dbt_cloud_job_run