import pandas as pd
from queue import Queue, Empty
from AWSIoTPythonSDK import MQTTLib

key_path = r"C:\keys/"

endpoint = "a37hmlo680f6sz-ats.iot.eu-central-1.amazonaws.com"
cert_path = key_path + "cert.pem.crt"
key_private_path = key_path + "PC.private.key"
ca_cert_path = key_path + "AmazonRootCA1.pem"

topic = "PLC_Tanks"

def publisher_thread(thread_timeout_s:float, plc_queue:Queue):
    '''
    Collect data until queue timeout runs out
    '''
    s7publisher = MQTTLib.AWSIoTMQTTClient("s7PLC_Publisher")
    s7publisher.configureEndpoint(endpoint, 8883)
    s7publisher.configureCredentials(ca_cert_path, key_private_path, cert_path)
    
    s7publisher.configureOfflinePublishQueueing(-1)  
    s7publisher.configureDrainingFrequency(2)  
    s7publisher.configureConnectDisconnectTimeout(10)  
    s7publisher.configureMQTTOperationTimeout(10)  
    
    off_condition = not s7publisher.connect()
    

    while not off_condition:
        try:
            plc_data = plc_queue.get(timeout=thread_timeout_s)
            if type(plc_data) is pd.DataFrame:
                publish_message = plc_data.to_json()
                s7publisher.publish(
                    topic = topic,
                    payload = publish_message,
                    QoS=0
                )
            elif plc_data == 'kill consumer':
                off_condition = True
        except Empty: 
            off_condition = True
        except AttributeError:
            print(f'PLC data might have wrong structure')
    else:
        s7publisher.disconnect()
        print('Publisher thread ended')
