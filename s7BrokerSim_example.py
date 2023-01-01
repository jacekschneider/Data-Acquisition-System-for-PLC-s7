import s7broker
import pandas as pd
import time
from threading import Thread
from queue import Queue, Empty

def consumer_thread(thread_timeout_s:float, plc_queue:Queue):
    
    # Collect the data until queue timeout runs out
    off_condition = False
    while not off_condition:
        try:
            plc_data = plc_queue.get(timeout=thread_timeout_s)
            if type(plc_data) is pd.DataFrame:
                print(f'Tank 1 level:{plc_data.iloc[0].Value}')
            elif plc_data == 'kill consumer':
                off_condition = True
        except Empty: 
            off_condition = True
        except AttributeError:
            print(f'PLC data might have wrong structure')
    else:
        print('Consumer thread ended')


CONSUMER_TIMEOUT_S = 10

broker = s7broker.BrokerSim('logs/plc_data.txt','ExchangeData.xlsx')
broker.auto_config()
plc_consumer_thread = Thread(target=consumer_thread, args=(CONSUMER_TIMEOUT_S, broker.broker_queue))

broker.start()
plc_consumer_thread.start()

try:
    while broker.is_alive() and plc_consumer_thread.is_alive():
        time.sleep(1)
except KeyboardInterrupt:
    broker.stop()
    broker.join()
    plc_consumer_thread.join()