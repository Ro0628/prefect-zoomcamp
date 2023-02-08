from prefect import flow
from prefect_github import GitHubCredentials
from prefect_github.repository import GitHubRepository
from etl_web_to_gcs import etl_web_to_gcs
from prefect.deployments import Deployment


github_repository_block = GitHubRepository.load("git-repository")
GitHubRepository.get_directory(github_repository_block,local_path="/Users/ronaldajohnson/Data Engineering/Projects/zoomcamp")

deployment = Deployment.build_from_flow(
        flow=etl_web_to_gcs,
        name="git-deploy",
        storage=github_repository_block,
        entrypoint="etl_web_to_gcs"
    )
#GitHubRepository.save("git-repository")

if __name__ == "__main__":
    deployment.apply()
