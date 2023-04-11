from fastapi import FastAPI
import requests
import time
import airflow_client.client as client
from airflow_client.client.api import dag_run_api
from airflow_client.client.model.dag_run import DAGRun
from airflow_client.client.model.error import Error
from pprint import pprint
import mysql.connector
import psycopg2
from elasticsearch import Elasticsearch



# Define the database connection parameters
db_host = "localhost"
db_port = "5432"
db_name = "data-db"
db_user = "postgres"
db_pass = "123456"
db_table = "Views"


app = FastAPI()

@app.get("/get/{user_id}")
async def history_get(user_id):
  
    # Connect to ElasticSearch
    es = Elasticsearch(['http://localhost:9200'])

    sql = f"select productid,timestamp from views where userid='{user_id}' order by timestamp desc limit 10"

    results = es.sql.query(body={'query': sql})
    if len(results["rows"]) == 0:
        return f"There is no history to show for {user_id}"
    
    else:
        return results["rows"]

@app.get("/delete/{user_id}/{product_id}")
async def history_delete(user_id,product_id):

    es = Elasticsearch(['http://localhost:9200'])

    query = {"query": {
                "bool": 
                {
                    "must": 
                    [
                        {"term": {"userid.keyword": user_id}},
                        {"term": {"productid.keyword": product_id}}
                    ]
                }
            }
         }

    result = es.delete_by_query(index='views', body=query)
    if(result["deleted"] != 0):
        return f"Product view history for {product_id} with the {user_id}  is deleted."
    else:
        return f"There is no product view history for {product_id} with the {user_id} " 


@app.get("/personalized/{user_id}")
async def personalize_bestsellers(user_id):
  
    data = await history_get(user_id)

    if type(data) is not  list:
        data = await bestsellers()
     
        return {"message": "There is no product view history for the given user. Here are the general best seller products",
             "Products": data} 
      
     
    products = "("
    count = 0
    for rows in data:
        if (count == 0):
            products = products + "'" + rows[0] + "'"
        else:
            products = products + "," + "'" + rows[0] + "'"  
        count = count + 1
        
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    sql =  f" SELECT product_id, category_id FROM products WHERE category_id IN (SELECT category_id FROM products WHERE product_id IN {products})) LIMIT 10"

    # Execute the SQL query to create the table
    cursor.execute(sql)
    rows = cursor.fetchall()
    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

    return  {"message": "Here are the personalized  best seller products",
                "Products": rows}





@app.get("/non-personalized/bestsellers")
async def bestsellers():

    airflow_configuration = client.Configuration(
        host = "http://localhost:8080/api/v1",
        username = 'admin',
        password = 'admin'
    )


    # Enter a context with an instance of the API client
    with client.ApiClient(airflow_configuration) as api_client:
        # Create an instance of the API class
        api_instance = dag_run_api.DAGRunApi(api_client)
        dag_id = "Bestsellers" # str | The DAG ID.
        dag_run = DAGRun(
            conf={},
            note="run batch bestsellers job",
        ) # DAGRun | 

        # example passing only required values which don't have defaults set
        try:

            # Trigger a new DAG run
            api_response = api_instance.post_dag_run(dag_id, dag_run)
            pprint(api_response)

            mydb = mysql.connector.connect(
            host="localhost",
            user="admin",
            password="admin",
            database="airflow_db"
            )

            mycursor = mydb.cursor()

            mycursor.execute("SELECT productid, user_count FROM non_pers_bestseller LIMIT 10")

            myresult = mycursor.fetchall()

            mycursor.close()

        except client.ApiException as e:
            print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)


    return myresult
