from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator, BashOperator
from utils import load_bertopic_retrain_conf

cfg = load_bertopic_retrain_conf()

with DAG(dag_id='bertopic-retrain', schedule_interval='0 0 1 * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:

    task_load = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-bertopic-load",
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

    task_retrain_and_save = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-bertopic-retrain",
        image=cfg.pipeline_args.docker_image,
        task_id="retrain",
        command=[
            "python3",
            "retrain.py"
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    restart_operator = BashOperator(
        task_id='reboot topic api',
        bash_command= 'curl -X POST "http://docker-socket-proxy:2375/containers/lblod-bertopic-api/restart"',

    )

    task_transform = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-bertopic-transform",
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

    task_save_topics = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-bertopic-save_topics",
        image=cfg.pipeline_args.docker_image,
        task_id="save_topics",
        command=[
            "python3",
            "save_topics.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_save_transform = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-bertopic-save_transform",
        image=cfg.pipeline_args.docker_image,
        task_id="save_documents",
        command=[
            "python3",
            "save_transform.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_load >> task_retrain_and_save >> restart_operator >> task_transform >> [task_save_transform, task_save_topics]
