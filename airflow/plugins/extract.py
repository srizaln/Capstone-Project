import psycopg2
import pandas as pd
from load import *

def connect():
    cons = "dbname='tugasakhir' user='postgres' host='host.docker.internal' password='rizal066196'"
    try:
        conn = psycopg2.connect(cons)
    except:
        print("Koneksi Gagal")
    return conn

#0 7 * * *
def extract_revenue():
    cur = connect().cursor()
    cur.execute('''SELECT a."ArtistId", a."Name" AS ArtistName, l."Title" AS TitleAlbum, t."Name" AS TrackName, il."Quantity"*il."UnitPrice" AS TotalPrice, il."InvoiceLineId", i."InvoiceId", i."InvoiceDate" FROM "Artist" a JOIN "Album" l ON l."ArtistId"=a."ArtistId" JOIN "Track" t ON l."AlbumId"=t."AlbumId" JOIN "InvoiceLine" il ON t."TrackId"=il."TrackId" JOIN "Invoice" i ON il."InvoiceId"=i."InvoiceId"''')
    record = cur.fetchall()

    dataframe = pd.DataFrame(
        record,
        columns = [
            'ArtistId',
            'ArtistName',
            'TitleAlbum',
            'TrackName',
            'TotalPrice',
            'InvoiceLineId',
            'InvoiceId',
            'InvoiceDate'
        ]
    )

    load_bq(dataframe, 'revenue')

#0 7 * * */3
def extract_song():
    cur = connect().cursor()
    cur.execute('''SELECT a."ArtistId", a."Name" AS ArtistName, l."AlbumId", l."Title" AS TitleAlbum, t."TrackId", t."Name" AS TrackName, t."UnitPrice" AS TrackUnitPrice, t."Bytes" AS TrackBytes, g."GenreId", g."Name" AS GenreName FROM "Artist" a JOIN "Album" l ON l."ArtistId"=a."ArtistId" JOIN "Track" t ON l."AlbumId"=t."AlbumId" JOIN "Genre" g ON t."GenreId"=g."GenreId"''')
    record = cur.fetchall()

    dataframe = pd.DataFrame(
        record,
        columns = [
            'ArtistId',
            'ArtistName',
            'AlbumId',
            'TitleAlbum',
            'TrackId',
            'TrackName',
            'TrackUnitPrice',
            'TrackBytes',
            'GenreId',
            'GenreName'
        ]
    )

    load_bq(dataframe, 'song')

#0 */3 * * *
def extract_transactions():
    cur = connect().cursor()
    cur.execute('''SELECT il."TrackId", il."InvoiceId", i."CustomerId", i."InvoiceDate", i."Total", CONCAT(c."FirstName", ' ', c."LastName") AS FullName, c."Address", c."City", c."State", c."Country", c."PostalCode", c."Email" FROM "InvoiceLine" il JOIN "Invoice" i ON il."InvoiceId"=i."InvoiceId" JOIN "Customer" c ON i."CustomerId"=c."CustomerId";''')
    record = cur.fetchall()

    dataframe = pd.DataFrame(
        record,
        columns = [
            'TrackId',
            'InvoiceId',
            'CustomerId',
            'InvoiceDate',
            'Total',
            'FullName',
            'Address',
            'City',
            'State',
            'Country',
            'PostalCode',
            'Email'
        ]
    )

    load_bq(dataframe, 'transactions')