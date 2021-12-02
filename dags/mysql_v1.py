from airflow import DAG
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


default_args = {
    'owner': 'Leo',  # DAG擁有者名稱, 通常為負責此DAG的人員
    # 每一次執行的 Task 是否會依賴於上次執行的 Task，如果是 False 的話，代表上次的 Task 如果執行失敗，這次的 Task 就不會繼續執行
    'depends_on_past': False,
    # Task 從哪個日期後開始可以被 Scheduler 排入排程
    'start_date': datetime(2021, 12, 28, 13, 48),
    'end_date': datetime(9999, 12, 31),  # Task 從哪個日期後，開始不被 Scheduler 放入排程
    # 一個 DAG Run 中的執行日期，只等於它「負責」的日期，不等於它實際被 Airflow 排程器執行的日期
    'schedule_interval': '@daily',
    'email': ['leo.du@eyesmedia.com.tw'],  # 如果 Task 執行失敗的話，要寄信給哪些人的 email
    'email_on_failure': False,  # 如果 Task 執行失敗的話，是否寄信
    'email_on_retry': False,  # 如果 Task 重試的話，是否寄信
    'retries': 3,  # 最多重試的次數
    # 'retryDelay': timedelta(seconds=5), # 每次重試中間的間隔
    # 'on_failure_callback': some_function, # Task 執行失敗時，呼叫的 function
    # 'on_success_callback': some_other_function, # Task 執行成功時，呼叫的 function
    # 'on_retry_callback': another_function, # Task 重試時，呼叫的 function
    # 'execution_timeout': timedelta(seconds=300), # Task 執行時間的上限
}

def check_col(ti):
    mysql = MySqlHook(mysql_conn_id='airflow_db')
    conn = mysql.get_conn()
    cursor = conn.cursor()

    yestr_sql = "select Mode from testdb.tb_airflowtestlog where CreateDate = date_add(CAST(NOW() as date), interval -1 day);"
    cursor.execute(yestr_sql)
    yestr_result = cursor.fetchone()

    today_sql = "select mode from testdb.tb_airflowtestlog where CreateDate = CAST(NOW() as date);"
    cursor.execute(today_sql)
    today_result = cursor.fetchone()

    msg = ''
    col_num = 0

    if yestr_result[0] != today_result[0]:
        col_num = today_result[0] - yestr_result[0]
        msg = f"本日新增{col_num}個欄位"
    else:
        msg = ''
    ti.xcom_push(key='send_msg', value=msg)

dag = DAG("mysql_v1"
        , default_args=default_args)



do_check_col = PythonOperator(
    task_id='check_col',
    python_callable=check_col,
    do_xcom_push=True,  # 讓xcom的value可以pull到與整個DAG共享
    dag=dag
)


branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda **context: 'send_notification' if len(str(
        context['ti'].xcom_pull(task_ids='check_col'))) > 3 else 'do_nothing',
    dag=dag,
)

do_send_notification = SlackAPIPostOperator(
    task_id='send_notification',
    token="xoxb-2569200031652-2572537451235-DjJVk6r7OzJlu0pr7iiRXkku",
    channel='#airflowcomic',
    text='''[{{ds}}]  {{ti.xcom_pull(key='send_msg',task_ids='check_col')}}''',
    dag=dag
)

do_nothing = DummyOperator(
    task_id='do_nothing',
    dag=dag
)

# define workflow(operator)
do_check_col >> branching >> [do_send_notification, do_nothing]
