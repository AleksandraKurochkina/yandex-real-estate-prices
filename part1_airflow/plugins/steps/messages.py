# part1_airflow/plugins/steps/messages.py

from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv
import os

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    load_dotenv()
    token = os.environ.get('TOKEN')
    chat_id = os.environ.get('CHAT_ID')
    hook = TelegramHook(telegram_conn_id='test',
                        token='',
                        chat_id='')
    run_id = context['run_id']
    
    message = f'Исполнение DAG с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    load_dotenv()
    token = os.environ.get('TOKEN')
    chat_id = os.environ.get('CHAT_ID')
    hook = TelegramHook(telegram_conn_id = 'test', token='',
                        chat_id='')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG c id={run_id} завершено с ошибкой! Вот больше инфы: {task_instance_key_str}'
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })
