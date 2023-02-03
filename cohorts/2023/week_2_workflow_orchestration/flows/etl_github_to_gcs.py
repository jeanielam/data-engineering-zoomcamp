from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow

github_block = GitHub.load("github-to-gcs-flow")

github_block.get_directory(
    from_path="cohorts/2023/week_2_workflow_orchestration/flows/parameterized_flows/etl_parent_flow.py",
)

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
)

if __name__ == "__main__":
    github_dep.apply()