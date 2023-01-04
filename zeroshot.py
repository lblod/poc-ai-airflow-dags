from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from utils.utils import load_zeroshot_config

cfg = load_zeroshot_config()

with DAG(dag_id='zeroshot', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(),
         catchup=False) as dag:
    task_load_taxo = KubernetesPodOperator(
        name="abb-load-bbc",
        image=cfg.pipeline_args.image,
        task_id="load-bbc",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_taxo_query,
            "bbc_taxo"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    # Tasks dynamically generated
    task_load = KubernetesPodOperator(
        name="abb-load-docs",
        image=cfg.pipeline_args.image,
        task_id="load-docs",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query,
            "export"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    # task_translate = KubernetesPodOperator(
    #     name="abb-translate",
    #     image=cfg.pipeline_args.image,
    #     task_id="translation_nl_to_en",
    #     cmds=["python3", "translate.py", "Helsinki-NLP/opus-mt-nl-en"],
    #     volume_mounts=cfg.pipeline_args.mount
    # )

    task_zs_bbc = KubernetesPodOperator(
        name="abb-zeroshot",
        image=cfg.pipeline_args.image,
        task_id="zeroshot_bbc",
        cmds=["python3", "zeroshot.py"],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save = KubernetesPodOperator(
        name="abb-save",
        image=cfg.pipeline_args.image,
        task_id="save",
        cmds=["python3", "save.py", cfg.pipeline_args.sparql_endpoint],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    # task_load >> task_translate >> task_zs_bbc >> task_save
    [task_load, task_load_taxo] >> task_zs_bbc >> task_save
