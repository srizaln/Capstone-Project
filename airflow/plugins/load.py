from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"./plugins/cde2022-370103-c4e72e13ff36.json"
client = bigquery.Client()

def load_bq(data, tabel):
    table_id = f"cde2022-370103.tugasakhir.{tabel}"

    job_config = bigquery.LoadJobConfig(
        autodetect = True,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )
    
    job.result()

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )