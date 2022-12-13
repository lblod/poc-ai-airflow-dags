from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from utils import load_embed_config

cfg = load_embed_config()

with DAG(dag_id='embed', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:
    task_load = KubernetesPodOperator(
        name="embed-load",
        image=cfg.pipeline_args.image,
        task_id="load",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        image_pull_policy="always",
        volume_mounts=cfg.pipeline_args.mount
    )

    task_embed = KubernetesPodOperator(
        name="embed-embedding",
        image=cfg.pipeline_args.image,
        task_id="embed",
        cmds=[
            "python3",
            "embed.py"
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save = KubernetesPodOperator(
        name="embed-save",
        image=cfg.pipeline_args.image,
        task_id="save",
        cmds=[
            "python3",
            "save.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_embed >> task_save
