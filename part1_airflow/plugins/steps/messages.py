# part1_airflow/plugins/steps/messages.py

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    from airflow.providers.telegram.hooks.telegram import TelegramHook
    hook = TelegramHook(telegram_conn_id='test',
                        token='7994940495:AAELYlRm8oXYj4si4gQedvhChTLUoBE34L4',
                        chat_id='-4672204489')
    run_id = context['run_id']
    
    message = f'Исполнение DAG с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4672204489',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    from airflow.providers.telegram.hooks.telegram import TelegramHook
    hook = TelegramHook(telegram_conn_id = 'test', token = '7994940495:AAELYlRm8oXYj4si4gQedvhChTLUoBE34L4', chat_id = '-4672204489')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG c id={run_id} завершено с ошибкой! Вот больше инфы: {task_instance_key_str}'
    hook.send_message({
        'chat_id': '-4672204489',
        'text': message
    })