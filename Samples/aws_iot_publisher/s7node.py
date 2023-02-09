import s7comm
import time
from threading import Thread
from iot_publisher import publisher_thread


PLC_IP='192.168.33.6'
DB_NUMBER = 1
INTERVAL_S = 1
CONSUMER_TIMEOUT_S = 10

# Create a broker and use necessary functions
s7Broker = s7comm.Broker('ExchangeData.xlsx')
print(s7Broker)
s7Broker.auto_config()
s7Broker.change_connection_options(PLC_IP, DB_NUMBER, INTERVAL_S)
plc_consumer_thread = Thread(target=publisher_thread, args=(CONSUMER_TIMEOUT_S, s7Broker.broker_queue))

s7Broker.start()
plc_consumer_thread.start()

try:
    while s7Broker.is_alive() and plc_consumer_thread.is_alive():
        time.sleep(1)
except KeyboardInterrupt:
    s7Broker.stop()
    s7Broker.join()
    plc_consumer_thread.join()