from ast import In
from dagster import job, op, get_dagster_logger, Field, Out, In
# from dsml4s8e import runner
from typing import Tuple


def get_op_by_nb_id(nb_id: str):
    config_schema = {
        "a": Field(
            str,
            description='nb params',
            default_value='default a'
            )
        }
    out = {
        "entity1": Out(str, description='from kafka topic entity1'),
        "entity2": Out(str),
        "unused": Out(str)
    }
    description = f'{nb_id}\nfull path'
    return {"config_schema": config_schema, "out": out, "description": description}


@op(**get_op_by_nb_id("load.notebook1"))
def run_load(context) -> Tuple[str, str, str]:

    # parameters to set to nb
    parameters = {
        'parameters': {'a': 1}}
    # parameters from UI
    parameters['parameters']['a'] = context.op_config["a"]

    nb_name = 'test.ipynb'
    parameters['parameters']['nb_full_name'] = nb_name
    context.log.info((__file__))
    get_dagster_logger().info((parameters))
    nb_output = 'test_out.ipynb'
    # runner.run_notebooks(nb_name, nb_output, parameters=parameters)

    out_nb = 'test.ipynb'
    get_dagster_logger().info(f"{out_nb}")
    # run_info = extract_results(nb_output)
    # entity1 = run_info.out_urls
    entity1 = 'entity1_url'
    entity2 = 'entity2_url'
    u = 'unused_url'
    return (entity1, entity2, u)


@op(
    description="""comp.notebook2""",
    ins={
        "entity1": In(str),
        "entity2": In(str)
    },
    out={},
)
def run_prep(context, entity1: str, entity2: str):
    context.log.info("run_prep")
    # locals() -> _resources_dict
    # make_resources_dict((locals())
    context.log.info(locals())


@job
def pipeline():
    # new run id
    res_urls = run_load()
    run_prep(*res_urls[:-1])


if __name__ == "__main__":
    result = pipeline.execute_in_process(run_config={
        'ops': {'run_load': {'config': {'a': 'a=?'}}}
    })
