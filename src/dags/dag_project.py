from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("example_bash_dag",
          schedule_interval=None,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
#t1 = BashOperator(
#    task_id='partition_overwrite',
#    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster  /lessons/partition.py 2022-05-25 /user/master/data/events /user/pgoeshard/data/events',
#        retries=3,
#        dag=dag)

cdm1 = SparkSubmitOperator(
                        task_id='d_27',
                        dag=dag,
                        name = "User_home_city",
                        #java_class = 
                        application ='/lessons/cdm1.py' ,
                        conn_id= 'yarn_conn',
                        #executor_cores = 2,      
                        #executor_memory = '2g',
                        #py_files = '/lessons/verified_tags_candidates.py',
                        conf={"spark.driver.maxResultSize": "20g"},                                                                 
                        application_args = ["2022-05-25", "27", 
                                            "/user/pgoeshard/data/events"
                                            ]
                        )

cdm2 = SparkSubmitOperator(
                        task_id='d_27',
                        dag=dag,
                        name = "Zones",
                        #java_class = 
                        application ='/lessons/cdm1.py' ,
                        conn_id= 'yarn_conn',
                        #executor_cores = 2,      
                        #executor_memory = '2g',
                        #py_files = '/lessons/verified_tags_candidates.py',
                        conf={"spark.driver.maxResultSize": "20g"},                                                                 
                        application_args = ["2022-05-25", "27", 
                                            "/user/pgoeshard/data/events"
                                            ]
                        )

cdm3 = SparkSubmitOperator(
                        task_id='d_27',
                        dag=dag,
                        name = "user_friend_suggest",
                        #java_class = 
                        application ='/lessons/cdm1.py' ,
                        conn_id= 'yarn_conn',
                        #executor_cores = 2,      
                        #executor_memory = '2g',
                        #py_files = '/lessons/verified_tags_candidates.py',
                        conf={"spark.driver.maxResultSize": "20g"},                                                                 
                        application_args = ["2022-05-25", "27", 
                                            "/user/pgoeshard/data/events"
                                            ]
                        )
cdm1 >> cdm2 >> cdm3
