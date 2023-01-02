import s7broker
import time
from threading import Thread
from consumer import consumer_thread


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