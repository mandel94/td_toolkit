from prefect import task, Flow, Parameter
from your_etl_module import ETLPipeline1, ETLPipeline2  # Assuming your ETLs are in different classes/modules

@task
def run_etl_pipeline_1():
    pipeline = ETLPipeline1()
    pipeline.run()

@task
def run_etl_pipeline_2():
    pipeline = ETLPipeline2()
    pipeline.run()

with Flow("Composite ETL Pipeline") as flow:
    # You can set dependencies between different pipelines if needed
    etl1 = run_etl_pipeline_1()
    etl2 = run_etl_pipeline_2()

flow.run()
