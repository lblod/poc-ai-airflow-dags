from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from utils import load_embed_config

cfg = load_embed_config()

with DAG(dag_id='embed', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:
    task_load = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="embed-load",
        image=cfg.pipeline_args.docker_image,
        task_id="load",
        command=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        network_mode="bridge",
        auto_remove=cfg.pipeline_args.auto_remove,
        force_pull=True,
        mounts=cfg.pipeline_args.mount
    )

    task_embed = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="embed-embedding",
        image=cfg.pipeline_args.docker_image,
        task_id="embed",
        command=[
            "python3",
            "embed.py"
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_save = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="embed-save",
        image=cfg.pipeline_args.docker_image,
        task_id="save",
        command=[
            "python3",
            "save.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_load >> task_embed >> task_save
