import s7broker
import time
from threading import Thread
from consumer import consumer_thread


PLC_IP='192.168.33.6'
DB_NUMBER = 1
INTERVAL_S = 1
CONSUMER_TIMEOUT_S = 10

# Create a broker and use necessary functions
broker = s7broker.Broker('ExchangeData.xlsx')
print(broker)
broker.auto_config()
broker.change_connection_options(PLC_IP, DB_NUMBER, INTERVAL_S)
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