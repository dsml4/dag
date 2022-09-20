from codecs import strict_errors
import sys
from typing import Tuple
from dagster import job, op, get_dagster_logger, Field, Out, In

sys.path.extend(['.', '..'])
from dsml4s8e import dag_params, runner, nb_parser


load_data_params = dag_params.NbOpParams('data_load.test')


@op(
    config_schema={
        "a": Field(
            int,
            description='nb params',
            default_value=100
            )
    },
    description=load_data_params.description,
    out=load_data_params.out
)
def run_load(context) -> Tuple[str, str]:

    # parameters to set to nb
    parameters = {
        'parameters': {'a': 1}}
    # parameters from UI
    parameters['parameters']['a'] = context.op_config["a"]

    nb_path = load_data_params.nb_path
    parameters['parameters']['nb_full_name'] = nb_path
    context.log.info((load_data_params.run_id))
    context.log.info((nb_path))
    context.log.info((parameters))
    component_cd = runner.make_cache_dir(
        load_data_params.compwd,
        load_data_params.run_id
        )
    context.log.info((component_cd))
    nb_out_path = runner.run_notebook(
        nb_path=nb_path,
        papermill_params=parameters,
        component_cd=component_cd
        )
    context.log.info((nb_out_path))
    results = nb_parser.get_results(nb_out_path)
    context.log.info((results))
    r = results['artefacts']
    data1_url = r['pipeline_example.data_load.test.data1']
    data2_url = r['pipeline_example.data_load.test.data2']
    return (data1_url, data2_url)


@op(
    description="""comp.notebook2""",
    ins={
        "entity1": In(str),
        "entity2": In(str)
    },
    out={},
)
def run_prep(context, entity1: str, entity2: str):
    context.log.info(entity1)
    context.log.info(entity2)


@job
def pipeline():
    load_data_params.run_id = runner.new_run_id()
    res_urls = run_load()
    run_prep(*res_urls)


if __name__ == "__main__":
    load_data_params.nb_path
    print(load_data_params.parameters)
