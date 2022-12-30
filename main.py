import s7broker
import pandas as pd
import time
from threading import Thread, Event
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
            print(f'PLC data might have a wrong structure')
    else:
        print('Consumer thread ended')



PLC_IP='192.168.33.6'
DB_NUMBER = 1
INTERVAL_S = 1
CONSUMER_TIMEOUT_S = 10

plc_queue = Queue()
stop_event = Event()

# Create a broker and use necessary functions
broker = s7broker.Broker('ExchangeData.xlsx')
broker.prepare_value_frame()
broker.compute_additional_offset()
broker.define_full_byte_range()


broker_thread = Thread(target=broker.broker_thread, args=(PLC_IP, DB_NUMBER, INTERVAL_S, plc_queue, stop_event))
plc_consumer_thread = Thread(target=consumer_thread, args=(CONSUMER_TIMEOUT_S, plc_queue))

broker_thread.start()
plc_consumer_thread.start()

try:
    while broker_thread.is_alive() and plc_consumer_thread.is_alive():
        time.sleep(1)
except KeyboardInterrupt:
    stop_event.set()
    broker_thread.join()
    plc_consumer_thread.join()
    
