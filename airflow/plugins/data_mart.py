from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"./plugins/cde2022-370103-c4e72e13ff36.json"
client = bigquery.Client()

def make_data(query, tabel):
    query_job = client.query(query)
    
    data = query_job.to_dataframe()

    table_id = f"cde2022-370103.datamart.{tabel}"

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

def pendapatan_penjualan():
    query = ("""
    SELECT InvoiceDate AS Tanggal, ROUND(SUM(TotalPrice),2) AS Total
    FROM `cde2022-370103.tugasakhir.revenue`
    GROUP BY InvoiceDate;
    """)

    make_data(query, 'pendapatan_penjualan')

def lokasi_customer():
    query = ("""
    SELECT Country, COUNT(*) AS Jumlah
    FROM `cde2022-370103.tugasakhir.transactions`
    GROUP BY Country
    ORDER BY Jumlah DESC;
    """)
    
    make_data(query, 'lokasi_customer')

def pendapatan_artist():
    query = ("""
    SELECT ArtistName, COUNT(TrackName) AS Penjualan, ROUND(SUM(TotalPrice),2) AS Pendapatan
    FROM `cde2022-370103.tugasakhir.revenue`
    GROUP BY ArtistName
    ORDER BY Pendapatan DESC;
    """)
    
    make_data(query, 'pendapatan_artist')

def artist_produktif():
    query = ("""
    SELECT ArtistName, GenreName, COUNT(GenreId) AS Jumlah
    FROM `cde2022-370103.tugasakhir.song`
    GROUP BY GenreName, ArtistName
    ORDER BY Jumlah DESC;
    """)
    
    make_data(query, 'artist_produktif')