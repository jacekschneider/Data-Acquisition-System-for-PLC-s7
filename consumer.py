import pandas as pd
from queue import Queue, Empty


def consumer_thread(thread_timeout_s:float, plc_queue:Queue):
    
    '''
    Collect data until queue timeout runs out
    '''
    off_condition = False
    while not off_condition:
        try:
            plc_data = plc_queue.get(timeout=thread_timeout_s)
            if type(plc_data) is pd.DataFrame:
                message = '''
                        Tank1           
Level                   {:3d}           
Discharge Flow          {:3d}           
Set Point               {:3d}           
Control Variable        {:3.2f}            
'''.format(
    plc_data.loc['iT1_LVL'].Value, 
    plc_data.loc['iT1_DIS_FL'].Value, 
    plc_data.loc['iT1_SP'].Value, 
    plc_data.loc['rT1_MV'].Value, 
    )
                print(message)
            elif plc_data == 'kill consumer':
                off_condition = True
        except Empty: 
            off_condition = True
        except AttributeError:
            print(f'PLC data might have wrong structure')
    else:
        print('Consumer thread ended')