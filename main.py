import s7broker
from threading import Thread
from queue import Queue, Empty


def consumer_thread(thread_timeout_s:float, plc_queue:Queue):
    
    # Collect the data until queue timeout runs out
    offCondition = False
    while not offCondition:
        try:
            plc_data = plc_queue.get(timeout=thread_timeout_s)
            print(f'Tank 1 level:{plc_data.iloc[0].Value}')
            pass
        except Empty: 
            offCondition = True
    else:
        print('Consumer thread ended')


PLC_IP='192.168.33.6'
DB_NUMBER = 1
INTERVAL_S = 1
CONSUMER_TIMEOUT_S = 3

plc_queue = Queue()

# Create a broker and use necessary functions
broker = s7broker.Broker('ExchangeData.xlsx')
broker.prepare_value_frame()
broker.compute_additional_offset()
broker.define_full_byte_range()


broker_thread = Thread(target=broker.broker_thread, args=(PLC_IP, DB_NUMBER, INTERVAL_S, plc_queue))
plc_consumer_thread = Thread(target=consumer_thread, args=(CONSUMER_TIMEOUT_S, plc_queue))

try:
    broker_thread.start()
    plc_consumer_thread.start()

except KeyboardInterrupt:
    broker_thread.join()
    plc_consumer_thread.join()

