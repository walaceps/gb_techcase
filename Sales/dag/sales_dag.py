import datetime
import airflow
import pandas as pd
import google.cloud.bigquery as bigquery
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud.exceptions import NotFound



default_args = {
    'owner': 'Walace Pereira de Paula e Silva',
    'start_date': datetime.datetime.now()- datetime.timedelta(days=1),
}


def load() -> bool:
    
    # Instanciando novos clientes da API gcloud
    client = bigquery.Client(project='techcase')

    # Pega os arquivos dos buckets
    df_2017 = pd.read_excel("gs://case-etl/Base_2017.xlsx")
    df_2018 = pd.read_excel("gs://case-etl/Base_2018.xlsx")
    df_2019 = pd.read_excel("gs://case-etl/Base_2019.xlsx")

    sales_df = pd.concat([df_2019, df_2018, df_2017])
    sales_df['ANO_VENDA'] = pd.to_datetime(sales_df['DATA_VENDA']).dt.year
    sales_df['MES_VENDA'] = pd.to_datetime(sales_df['DATA_VENDA']).dt.month

    dataset_id = "Sales_Data"

    # Verifica se o dataset existe e cria se necessario

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
    
    sales_table_id = "techcase.Sales_Data.sales"
    tabela1_table_id = "techcase.Sales_Data.tabela_1"
    tabela2_table_id = "techcase.Sales_Data.tabela_2"
    tabela3_table_id = "techcase.Sales_Data.tabela_3"
    tabela4_table_id = "techcase.Sales_Data.tabela_4"

    # Faz o load dos dados para o bigquery
    try:

        tabela_1 = sales_df.groupby(["ANO_VENDA","MES_VENDA",])["QTD_VENDA"].sum().reset_index()
        tabela_2 = sales_df.groupby(["MARCA","LINHA",])["QTD_VENDA"].sum().reset_index()
        tabela_3 = sales_df.groupby(["MARCA","ANO_VENDA","MES_VENDA"])["QTD_VENDA"].sum().reset_index()
        tabela_4 = sales_df.groupby(["LINHA","ANO_VENDA","MES_VENDA"])["QTD_VENDA"].sum().reset_index()

        job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')

        job = client.load_table_from_dataframe(sales_df, sales_table_id, job_config = job_config)
        job.result()

        job = client.load_table_from_dataframe(tabela_1, tabela1_table_id, job_config = job_config)
        job.result()

        job = client.load_table_from_dataframe(tabela_2, tabela2_table_id, job_config = job_config)
        job.result()

        job = client.load_table_from_dataframe(tabela_3, tabela3_table_id, job_config = job_config)
        job.result()

        job = client.load_table_from_dataframe(tabela_4, tabela4_table_id, job_config = job_config)
        job.result()

    except Exception as e:
        print("Fail to Write data")
        print(e)
        return False
    
    return True


with airflow.DAG('sales_etl_dag', schedule_interval= "0 0 * * *", default_args = default_args, catchup=False) as dag:

    begin = DummyOperator(
        task_id='begin'
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
    begin >> load_data >> end