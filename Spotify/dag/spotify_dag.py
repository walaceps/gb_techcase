import datetime
import airflow
import requests
import json
import pandas as pd
import google.cloud.bigquery as bigquery
import google.cloud.storage as storage
from urllib.parse import urlencode
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud.exceptions import NotFound


# Variaveis de ambiente para validar o token do spotify
spotify_etl_config = Variable.get("spotify_etl_dag_vars", deserialize_json=True)
SPOTIFY_SECRET = spotify_etl_config["spotify_secret"]

default_args = {
    'owner': 'Walace Pereira de Paula e Silva',
    'start_date': datetime.datetime.now()- datetime.timedelta(days=1),
}


def extract() -> bool:
    
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SPOTIFY_SECRET}"
    }

    #Request para os 50 shows com pesquisa data hackers.

    search = "data hackers"
    request = requests.get(f"https://api.spotify.com/v1/search?q=`{search}`&type=show&limit=50", headers = headers)

    # Transformando o resultado da requisição em um objeto JSON
    show_data = request.json()

    #Request para os 50 episodes do shows DATA HACKERS.

    search = "Data Hackers"
    request = requests.get(f"https://api.spotify.com/v1/search?q=`{search}`&type=episode&limit=50", headers = headers)

    episodes_data = request.json()


    #Request para os 50 episodes do shows Data Hackers com o grupo Boticario.

    search = "Data Hackers Grupo Boticario"
    request = requests.get(f"https://api.spotify.com/v1/search?q=`{search}`&type=episode&limit=50", headers = headers)

    episodes_gb_data = request.json()

    #Persistencia do arquivo raw da request em Bucket

    try:
        client = storage.Client(project="techcase")
        bucket = client.get_bucket("case-etl")

        blob = bucket.blob("raw.tabela5_request.json")
        blob.upload_from_string(data=json.dumps(show_data),content_type='application/json')

        blob = bucket.blob("raw.tabela6_request.json")
        blob.upload_from_string(data=json.dumps(episodes_data),content_type='application/json')

        blob = bucket.blob("raw.tabela7_request.json")
        blob.upload_from_string(data=json.dumps(episodes_gb_data),content_type='application/json')
    
    except Exception as e:
        print(e)
        return False

    return True

def transform() -> bool:


    client = storage.Client(project="techcase")
    bucket = client.get_bucket("case-etl")
    tabela5_blob = bucket.blob('raw.tabela5_request.json')
    tabela6_blob = bucket.blob('raw.tabela6_request.json')
    tabela7_blob = bucket.blob('raw.tabela7_request.json')

    show_data = json.loads(tabela5_blob.download_as_string(client=None))
    episode_data = json.loads(tabela6_blob.download_as_string(client=None))
    episode_gb_data = json.loads(tabela7_blob.download_as_string(client=None))


    #Normalizacao dos json em tabelas a serem armazenadas como backup e para load

    show_id = []
    show_name = []
    show_description = []
    show_total_episodes = []

    for show in show_data["shows"]["items"]:
        show_id.append(show["id"])
        show_name.append(show["name"])
        show_description.append(show["description"])
        show_total_episodes.append(show["total_episodes"])

    show_dict = {
        "id" : show_id,
        "name" : show_name,
        "description" : show_description,
        "total_episodes" : show_total_episodes,
    }

    show_df = pd.DataFrame(show_dict, columns=["id", "name", "description", "total_episodes"])

    episode_id = []
    epsiode_name = []
    epsiode_description = []
    episode_release_date = []
    episode_duration = []
    episode_language = []
    episode_explicit = []
    episode_type = []

    for episode in episode_data["episodes"]["items"]:
        episode_id.append(episode["id"])
        epsiode_name.append(episode["name"])
        epsiode_description.append(episode["id"])
        episode_release_date.append(episode["release_date"])
        episode_duration.append(episode["duration_ms"])
        episode_language.append(episode["language"])
        episode_explicit.append(episode["explicit"])
        episode_type.append(episode["type"])

    episode_dict = {
        "id" : episode_id,
        "name" : epsiode_name,
        "description" : epsiode_description,
        "release_date" : episode_release_date,
        "duration_ms" : episode_duration,
        "language" : episode_language,
        "explicit" : episode_explicit,
        "type" : episode_type
    }

    episode_df = pd.DataFrame(episode_dict, columns=["id", "name", "description", "release_date", "duration_ms", "language", "explicit", "type" ])


    episode_id = []
    epsiode_name = []
    epsiode_description = []
    episode_release_date = []
    episode_duration = []
    episode_language = []
    episode_explicit = []
    episode_type = []

    for episode in episode_gb_data["episodes"]["items"]:
        episode_id.append(episode["id"])
        epsiode_name.append(episode["name"])
        epsiode_description.append(episode["id"])
        episode_release_date.append(episode["release_date"])
        episode_duration.append(episode["duration_ms"])
        episode_language.append(episode["language"])
        episode_explicit.append(episode["explicit"])
        episode_type.append(episode["type"])

    episode_gb_dict = {
        "id" : episode_id,
        "name" : epsiode_name,
        "description" : epsiode_description,
        "release_date" : episode_release_date,
        "duration_ms" : episode_duration,
        "language" : episode_language,
        "explicit" : episode_explicit,
        "type" : episode_type
    }

    episode_gb_df = pd.DataFrame(episode_gb_dict, columns=["id", "name", "description", "release_date", "duration_ms", "language", "explicit", "type" ])

    #Persistencia dos arquivo transformed como csv em Bucket


    try:
        bucket.blob("transformed.tabela5_request.csv").upload_from_string(show_df.to_csv(), content_type='text/csv')
        
        bucket.blob("transformed.tabela6_request.csv").upload_from_string(episode_df.to_csv(), content_type='text/csv')
        
        bucket.blob("transformed.tabela7_request.csv").upload_from_string(episode_gb_df.to_csv(), content_type='text/csv')
    
    except Exception as e:
        print(e) 
        return False

    return True


def load() -> bool:

    client = bigquery.Client(project="techcase")
    dataset_id = "Spotify_Data"

    # Checa se o dataset existe e cria se necessario
    try:
        client.get_dataset(dataset_id)
        print(f"O dataset {dataset_id} já existe!")
    except NotFound:
        try:
            dataset = bigquery.Dataset(f"{client.project}.Spotify_Data")
            client.create_dataset(dataset, timeout=30)
            print(f"Dataset '{dataset_id}' criado com sucesso!")
        except Exception as e:
            print(e)
            return False
        
    # Define todos os tables ids
    
    tabela5_table_id = "techcase.Spotify_Data.tabela_5"
    tabela6_table_id = "techcase.Spotify_Data.tabela_6"
    tabela7_table_id = "techcase.Spotify_Data.tabela_7"


    #Persiste os dados transformados no bigquery
    try:

        tabela_5 = pd.read_csv("gs://case-etl/transformed.tabela5_request.csv")
        tabela_6 = pd.read_csv("gs://case-etl/transformed.tabela6_request.csv")
        tabela_7 = pd.read_csv("gs://case-etl/transformed.tabela7_request.csv")

        job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')

        job = client.load_table_from_dataframe(tabela_5, tabela5_table_id, job_config = job_config)
        job.result()

        job = client.load_table_from_dataframe(tabela_6, tabela6_table_id, job_config = job_config)
        job.result()

        job = client.load_table_from_dataframe(tabela_7, tabela7_table_id, job_config = job_config)
        job.result()

    except Exception as e:
        print("Fail to Write data")
        print(e)
        return False

    return True


with airflow.DAG('spotify_etl_dag', schedule_interval= "0 0 * * *", default_args = default_args, catchup=False) as dag:

    begin = DummyOperator(
        task_id='begin'
    )

    extract_data = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract,
    )

    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform,

    )
    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable = load,
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    # Declara a ordem das tasks
    begin >> extract_data >> transform_data >> load_data >> end