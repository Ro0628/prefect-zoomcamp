from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Transforming to Data Frame")
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="ro_dezoomcamp.yellow_rides",
        project_id="my-rides-ro",
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year, month, color):

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    print(f"Rows processed: {df.shape[0]}")
    write_bq(df)


@flow()
def etl_parent_flow(color: str = "yellow", months: list[int] = [1,2], year: int = 2021):
    for month in months:
        etl_gcs_to_bq(year, month, color)
    
if __name__ == "__main__":
    color="yellow"
    months=[2,3]
    year=2019
    etl_parent_flow(color,months, year)

