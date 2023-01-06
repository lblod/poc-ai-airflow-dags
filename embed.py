from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from utils import load_embed_config

cfg = load_embed_config()

with DAG(dag_id='embed', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:
    task_load = KubernetesPodOperator(
        namespace="abb",
        name="embed-load",
        image=cfg.pipeline_args.image,
        task_id="load",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount,
        image_pull_policy="Always"
    )

    task_embed = KubernetesPodOperator(
        namespace="abb",
        name="embed-embedding",
        image=cfg.pipeline_args.image,
        task_id="embed",
        cmds=[
            "python3",
            "embed.py"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save = KubernetesPodOperator(
        namespace="abb",
        name="embed-save",
        image=cfg.pipeline_args.image,
        task_id="save",
        cmds=[
            "python3",
            "save.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_embed >> task_save
