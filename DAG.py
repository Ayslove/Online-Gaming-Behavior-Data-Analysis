'''
===============================================================
Objective: Program ini dibuat untuk melakukan automasi proses perpindahan data 
menggunakan airflow webserver. 
Prosesnya adalah:
1. Data yang diperoleh dari external link diupload ke postgres
2. Dari postgres data diambil dan disave menjadi data_raw
3. Data raw diproses melalui data clean dan disave menjadi data_clean
4. Data clean diupload ke elasticsearch untuk dilakukan visualisasi menggunakan kibana
Keempat proses di atas, dilakukan secara squential. Proses di atas juga ditrigger
melalui airflow webserver.
===============================================================
'''

# Import library
'''
Library yang disiapkan utamanya berupa library untuk proses airflow dengan DAG dan 
execution order dari proses DAG itu sendiri
'''
from airflow.models import DAG # DAG airflow
from airflow.operators.python import PythonOperator # Execution order DAG
import datetime as dt # Datetime
from sqlalchemy import create_engine # Database connection
import pandas as pd # Data loading
from elasticsearch import Elasticsearch # NoSQL tools
import re # Regex untuk cleaning data

# Global variable db
database = 'ayslove_m3'
username = 'ayslove_m3'
password = 'ayslove_m3'
host = 'postgres'

# Koneksi ke db
postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
engine = create_engine(postgres_url)
conn = engine.connect()

# Fungsi upload data ke postgres
def csv_to_postgres():
    '''
    Kode ini untuk upload data csv raw dari external link ke postgres dengan 
    nama table yang sudah ditentukan yaitu table_m3.
    Rincian proses beserta environment name:

    postgres:
    server name: ayslove_m3
    database name: ayslove_m3
    table name: table_m3

    Setelah data di download dari kaggle, data diletakkan dalam folder dags
    folder ini terbentuk ketika menjalankan proses docker compose. Selanjutnya pada
    sesi csv_to_postgres data yang telah didownload dan di letakkan pada folder dags
    akan dilakukan proses upload ke server database postgres.
    '''
    # Fungsi upload file ke postgres
    df = pd.read_csv('/opt/airflow/dags/online_gaming_behavior_dataset.csv')
    df.to_sql('table_m3',conn,
              index=False,
              if_exists='replace')
    
    print(conn)

# Fungsi ambil data dari postgres
def postgres_to_csv():
    '''
    Setelah data berhasil diupload ke postgres dengan nama table_m3.
    Pada proses postgres_to_csv atau proses untuk menarik data yang sudah diupload ke postgres
    disini proses yang dilakukan adalah mengambil data dan merubah nama dataset menjadi data_raw.

    Proses dan environment name:
    
    postgres:
    server name: ayslove_m3
    database name: ayslove_m3
    table name: table_m3

    Proses:
    1. Data ditarik dari server postgres menggunakan query yang dapat dibaca postgres
    2. variable conn adalah koneksi antara vscode dengan postgres
    3. Data disave ke dalam folder dags dengan nama data_raw.csv
        *index=False artinya ketika saving, index tidak diikutsertakan*
    '''
    # Fungsi ambil data dari postgres
    df = pd.read_sql_query("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/data_raw.csv',sep=',',index=False)

# Fungsi pembersihan data
def data_cleaning():
    '''
    Selanjutnya pada proses data_cleaning, data sebelumnya yang sudah disave menjadi
    data_raw.csv akan dilakukan proses cleaning data. Cleaning data dilakukan menggunakan
    syntax yang terdapat pada library pandas. Untuk prosesnya sendiri sudah diberikan di atas
    setiap sesi pandas dataframe atau df.
    '''
    # Read data_raw
    df = pd.read_csv('/opt/airflow/dags/data_raw.csv')

    # Rename column dengan emisahkan nama kolom, contoh: PlayerID menjadi Player I D, PlayerLevel menjadi Player Level
    df.columns = df.columns.map(lambda x: ' '.join(re.findall(r'[A-Z][a-z]*', x)))

    # Rename Player I D menjadi Player ID
    df.rename(columns={'Player I D': 'Player ID'},inplace=True)

    # Mengganti spasi antara kata pada nama kolom menjadi _, contoh Player ID menjadi Player_ID
    df.columns = df.columns.str.replace(' ','_')

    # Merubah nama kolom menjadi huruf kecil keseluruhannya, contoh Player_ID menjadi player_id
    df.columns = df.columns.str.lower()

    # Hapus whitespace pada kolom jika ada
    df.columns = df.columns.str.strip()

    # Drop missing values jika ada
    df.dropna(inplace=True)

    # Drop duplicate jika ada
    df.drop_duplicates(inplace=True)

    # Reset index setelah proses drop, supaya index tidak berantakan dan tetap urut
    df.reset_index(inplace=True,drop=True)

    # Rubah tipe data pada kolom berikut menjadi string
    df['in_game_purchases'] = df['in_game_purchases'].astype(str)

    # Rubah 0 dan 1 menjadi no dan yes agar lebih mudah dibaca ketika divisualisasikan
    df['in_game_purchases'] = df['in_game_purchases'].map({'0': 'No', '1': 'Yes'})

    # Merapihkan angka dibelakang koma menjadi 2 angka.
    df['play_time_hours'] = df['play_time_hours'].astype(float).round(2)

    # Save data yang sudah clean
    df.to_csv('/opt/airflow/dags/data_clean.csv',sep=',',index=False)

# Fungsi upload data ke elasticsearch
def csv_to_es():
    '''
    Pada proses csv_to_es data disiapkan untuk diupload ke elasticsearch, 
    data yang digunakan adalah data yang sudah dibersihkan, hal ini dilakukan
    guna untuk data visualisasi di kibana.
    '''
    # Define server elasticsearch ke variable es
    es = Elasticsearch('http://elasticsearch:9200')
    
    # Read data yang akan diupload ke elasticsearch
    df = pd.read_csv('/opt/airflow/dags/data_clean.csv')

    # Looping untuk merubah data menjadi dictionary
    for i, r in df.iterrows():
        doc = r.to_dict()
        # Beri nama index supaya bisa diload di kibana.
        res = es.index(index='index_m3',id=i+1,body=doc)
        print(f'Response from Elasticsearch: {res}')

# Define default_args    
default_args = {
    'owner':'Ayslove',
    'start_date': dt.datetime(2024,7,10,6,30)
}

# DAG definition
with DAG(

    '''
    Fungsi ini adalah fungsi untuk automasi batch processing. Automasi dilakukan setiap jam 6.30
    default_args menggunakan data yang sudah didefinisikan di atas. catchup=False, artinya jika
    pada start_date didefinisikan jauh dari saat unpause atau saat trigger DAG, maka DAG akan
    berjalan dari start_date hingga hari dimana DAG ditrigger, hal ini terjadi jika catchup dibuat
    menjadi True. Pada projek kali ini karena proses penyelesaian projek dilakukan di hari yang sama
    maka catchup di set menjadi False.
    '''

    'P2M3_Syihabuddin_Ahmad_DAG',
    description='DAG Milestone_3',

    # Schedule untuk dirun setiap jam 6:30
    schedule_interval='30 6 * * *',
    default_args=default_args,
    catchup=False
) as dag:
    # DAG Task definition
    task_csv_to_pg = PythonOperator(task_id = 'csv_to_pg',
                                    python_callable = csv_to_postgres)
    task_pg_to_csv = PythonOperator(task_id = 'pg_to_csv',
                                    python_callable=postgres_to_csv)
    task_data_cleaning = PythonOperator(task_id = 'data_cleaning',
                                        python_callable=data_cleaning)
    task_csv_to_es = PythonOperator(task_id = 'csv_to_es',
                                    python_callable=csv_to_es)
    
    # DAG task execution order
    task_csv_to_pg >> task_pg_to_csv >> task_data_cleaning >> task_csv_to_es