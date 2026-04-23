
# from airflow import DAG
# from airflow.providers.docker.operators.docker import DockerOperator
# from docker.types import Mount
# import sys
# from datetime import datetime, timedelta

# sys.path.append("/opt/airflow/api-request")


# default_args = {
#     "description": "A DAG to orchestrate dbt transformed data",
#     "start_date": datetime(2026, 3, 10),
#     "catchup": False
# }
# dag = DAG(
#     dag_id="flight-dbt-orchetrtaor",
#     default_args=default_args,
#     schedule=timedelta(minutes=1),
# )

# with dag:
#     task2 = DockerOperator(
#         task_id="transform_data_task",
#         image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
#         command="run",
#         mounts=[
#             Mount(source='/home/nyaugenya/dev/dataenginering/weatherextact/weather-data-project/dbt/flight_project',
#                   target='/usr/app', type='bind'),
#             Mount(source='/home/nyaugenya/dev/dataenginering/weatherextact/weather-data-project/dbt/profiles.yml',
#                   target='/root/.dbt/profiles.yml', type='bind')
#         ],
#         network_mode="weather-data-project_flight-network",
#         docker_url='unix:///var/run/docker.sock',
#         auto_remove='success'
#     )
