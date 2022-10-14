from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from utils import load_zeroshot_config

cfg = load_zeroshot_config()

with DAG(dag_id='zeroshot', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(),
         catchup=False) as dag:
    task_load_taxo = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-load-bbc",
        image=cfg.pipeline_args.docker_image,
        task_id="load-bbc",
        command=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_taxo_query,
            "bbc_taxo"
        ],
        network_mode=cfg.pipeline_args.network_mode,
        force_pull=True,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    # Tasks dynamically generated
    task_load = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-load-docs",
        image=cfg.pipeline_args.docker_image,
        task_id="load-docs",
        command=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query,
            "export"
        ],
        network_mode=cfg.pipeline_args.network_mode,
        force_pull=True,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    # task_translate = DockerOperator(
    #     docker_url=cfg.pipeline_args.docker_host,
    #     container_name="abb-translate",
    #     image=cfg.pipeline_args.docker_image,
    #     task_id="translation_nl_to_en",
    #     command=["python3", "translate.py", "Helsinki-NLP/opus-mt-nl-en"],
    #     network_mode=cfg.pipeline_args.network_mode,
    #     auto_remove=cfg.pipeline_args.auto_remove,
    #     mounts=cfg.pipeline_args.mount
    # )

    task_zs_bbc = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-zeroshot",
        image=cfg.pipeline_args.docker_image,
        task_id="zeroshot_bbc",
        command=["python3", "zeroshot.py"],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    task_save = DockerOperator(
        docker_url=cfg.pipeline_args.docker_host,
        container_name="abb-save",
        image=cfg.pipeline_args.docker_image,
        task_id="save",
        command=["python3", "save.py", cfg.pipeline_args.sparql_endpoint],
        network_mode=cfg.pipeline_args.network_mode,
        auto_remove=cfg.pipeline_args.auto_remove,
        mounts=cfg.pipeline_args.mount
    )

    # task_load >> task_translate >> task_zs_bbc >> task_save
    [task_load, task_load_taxo] >> task_zs_bbc >> task_save
