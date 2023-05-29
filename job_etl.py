from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import requests
import os 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 29),
}

dag = DAG('data_processing', default_args=default_args, schedule_interval='@daily')


def define_checkpoint():
    if not os.path.exists('/home/projeto_bix/checkpoint.txt'):
        checkpoint = 0
    else:
        with open('/home/projeto_bix/checkpoint.txt') as f:
            checkpoint = f.read()
    return checkpoint

checkpoint = define_checkpoint()  


def create_or_replace_txt_file(checkpoint):
    file_path = '/home/projeto_bix/checkpoint.txt'
    content = str(checkpoint)
    with open(file_path, 'w') as file:
        file.write(content)

        
def extract_rdbms_data():
    '''
    Extract data from Postgres in a pandas df, filtering by a checkpoint var (max(id) of last executing)
    '''
    # connect to source PostgresDB that was parameterized in Airflow
    postgres_conn = PostgresHook(postgres_conn_id="postgresDB")

    # query
    query = f"SELECT id_venda, id_funcionario, id_categoria, data_venda, venda FROM venda WHERE id_venda > {checkpoint}"

    # save in df and cast date
    results = postgres_conn.get_pandas_df(query)
    results['data_venda'] = pd.to_datetime(results['data_venda'])

    # double-checking checkpoint var
    if results.empty:
        validation_query = "SELECT max(id_venda) FROM venda"
        validation = int(postgres_conn.get_pandas_df(validation_query).values)
        if validation == int(checkpoint):
            raise ValueError("There's no new data to load")  # Raise an exception
        else:
            raise ValueError("Check the checkpoint var")  # Raise an exception
    else:
        return results

query_venda = extract_rdbms_data()
max_id = query_venda['id_venda'].max()
unique_id_funcionario = query_venda['id_funcionario'].unique()

def download_parquet_file(url, filename):
    '''
    Func that downloads a parquet_file from the URL
    '''
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)

def extract_parquet_data():
    '''
    Invokes download_parquet_file() func and extracts data from parquet file,
    then saves it in a pandas df and renames the id column to id_categoria
    '''
    url = 'https://storage.googleapis.com/challenge_junior/categoria.parquet'
    filename = 'categoria.parquet'
    download_parquet_file(url, filename)

    df = pd.read_parquet(filename)
    new_df = df.rename(columns={"id": "id_categoria"})
    os.remove('categoria.parquet')
    return new_df
    
df_parquet = extract_parquet_data()

def extract_api_data(id_array):
    '''
    Consumes an array of unique id_funcionario to request the name of each employee from the API
    Returns a dictionary with the employee names and their corresponding ids
    '''
    base_url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id="
    api_list = []

    for id in id_array:
        url = f"{base_url}{id}"
        response = requests.get(url)
        data = response.text
        employee_data = {'nome_funcionario': data, 'id_funcionario': id}
        api_list.append(employee_data)

    return api_list

df_api = extract_api_data(unique_id_funcionario)

def merge_data():
    # merge the 3 df's
    df = pd.merge(query_venda, df_parquet, on='id_categoria')
    df_api_df = pd.DataFrame(df_api)  # Convert df_api to DataFrame
    transformed_df = pd.merge(df, df_api_df, on='id_funcionario')
    return transformed_df

df_final = merge_data()
def load_data_to_dw():
    '''
    Load the data from df_final pandas df to the destination DW
    '''
    # connect to destination PostgresDB that was parameterized in Airflow
    dw_conn = PostgresHook(postgres_conn_id="dw_dest")

    ## following dimensional modeling, 1st load the dimensional tables

    # load data into the funcionarios table (dimensional table)
    funcionarios_data = df_final[['nome_funcionario', 'id_funcionario']].drop_duplicates()
    dw_conn.insert_rows(table='dw_projeto.funcionarios', rows=funcionarios_data.values.tolist())

    # load data into the categoria table (dimensional table)
    categoria_data = df_final[['nome_categoria', 'id_categoria']].drop_duplicates()
    dw_conn.insert_rows(table='dw_projeto.categoria', rows=categoria_data.values.tolist())

    # create the calendario table (dimensional table) based on distinct dates in df_final
    calendario_data = df_final[['data_venda']].drop_duplicates().sort_values('data_venda')

    # slicing data components
    calendario_data['dia'] = calendario_data['data_venda'].dt.day
    calendario_data['mes'] = calendario_data['data_venda'].dt.month
    calendario_data['ano'] = calendario_data['data_venda'].dt.year
    calendario_data['bimestre'] = (calendario_data['mes'] - 1) // 2 + 1

    # load data into the calendario table
    dw_conn.insert_rows(table='dw_projeto.calendario', rows=calendario_data.values.tolist())

    # lastly load data into the vendas table (factual table)
    vendas_data = df_final[['venda', 'id_funcionario', 'id_categoria', 'data_venda','id_venda']]
    dw_conn.insert_rows(table='dw_projeto.vendas', rows=vendas_data.values.tolist())






with dag:
    
    t1 = PythonOperator(
        task_id='define_checkpoint',
        python_callable=define_checkpoint
    )

    
    t2 = PythonOperator(
        task_id='extract_rdbms_data',
        python_callable=extract_rdbms_data
    )

    t3 = PythonOperator(
        task_id='extract_parquet_data',
        python_callable=extract_parquet_data
    )

    t4 = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api_data,
        op_args=[unique_id_funcionario]
    )

    t5 = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data
    )

    t6 = PythonOperator(
        task_id='load_data_to_dw',
        python_callable=load_data_to_dw
    )
    
    t7 = PythonOperator(
        task_id='create_or_replace_txt_file',
        python_callable=create_or_replace_txt_file,
        op_args =[max_id]
    )
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
