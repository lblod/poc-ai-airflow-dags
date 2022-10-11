from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from utils import load_bertopic_transform_conf

cfg = load_bertopic_transform_conf()

with DAG(dag_id='bertopic-transform', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:

    task_load = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-embed-bertopic-load",
        image=cfg.pipeline_args.docker_image,
        task_id="load",
        command=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        network_mode=cfg.pipeline_args.network_mode,
        force_pull=True,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_transform = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-embed-bertopic-transform",
        image=cfg.pipeline_args.docker_image,
        task_id="transform",
        command=[
            "python3",
            "transform.py"
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_save = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-embed-bertopic-save",
        image=cfg.pipeline_args.docker_image,
        task_id="save",
        command=[
            "python3",
            "save_transform.py",
            cfg.pipeline_args.sparql_endpoint
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_load >> task_transform >> task_save
