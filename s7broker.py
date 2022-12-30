import pandas as pd
import numpy as np
import snap7
import time
import socket
from queue import Queue
from threading import Event, Thread


s7_bytes_to_read = {
    'Int'  : 2,
    'Real' : 4,
    'Bool' : 0,
}

s7_bits_to_read = {
    'Int'  : 0,
    'Real' : 0,
    'Bool' : 1,    
}

# ONLY BOOL TESTED
s7_additional_offset = {
    'Int'  : 2,
    'Real' : 4,
    'Bool' : 0,     
}

def s7clear_logs(path:str):
    try:
        open(path,'w').close()
    except:
        print(f'Could not clear a file on path: {path}')
        
def s7frame_get_byte(buffer:bytearray, index:int):
    return buffer[index]

def s7frame_get_bytes(s7frame:bytearray, indexStart:int, indexStop:int):
    return s7frame[indexStart:indexStop]

def s7frame_get_bit(s7frame:bytearray, indexByte:int, indexBit:int):
    '''
    Unpack byte and get bit
    '''
    byte = np.array(s7frame[indexByte], dtype='uint8')
    bits = np.unpackbits(byte)
    bit = bits[indexBit] if 0<=indexBit<=7 else None
    return bit

def s7buffer_to_value(buffer, type:str):
    '''
    s7 defines big-endian coding
    '''
    value = None
    if type=='Int':
        value = np.frombuffer(buffer, dtype='>i2')
    elif type=='Real':
        value = np.frombuffer(buffer, dtype='>f')
    return value[0]

def s7frame_extract(s7frame:bytearray, offset:float, data_type:str):
    '''
    Extract value from s7frame
    Offset is an indicator for value's data type 
    '''
    bytes_to_read = 0
    bits_to_read = 0
    bytes = None
    bit = None
    value = None

    # Separate the number and its floating point
    byte_start_index = int(offset)
    bit_start_index = np.ceil((offset*10-byte_start_index*10)).astype('uint8')

    # Change data amount to read according to the s7 data types    
    bytes_to_read = s7_bytes_to_read[data_type]
    bits_to_read = s7_bits_to_read[data_type]
    
    # Get binary data and transform it to the actual value
    if bytes_to_read>0:
        bytes = s7frame_get_bytes(s7frame, byte_start_index, byte_start_index + bytes_to_read)
        value = s7buffer_to_value(bytes, data_type)
    elif bits_to_read==1:
        bit = s7frame_get_bit(s7frame, byte_start_index, bit_start_index)
        value = bit      
    return value


class Broker(Thread):

    '''
    Broker class
    It provides a small example of data exchange mechanism between Python and a PLC
    
    Arguments required:
        - config_file_path (the file contains specified TIA portal db interface)
    '''

    def __init__(self, config_file_path:str, *args, **kwargs):
        '''
        Save config file path
        Initialize s7 client
        '''
        super().__init__(*args, **kwargs)
        self.config_file_path = config_file_path
        self.plc_client = snap7.client.Client()
        self.broker_queue = Queue()
        self.broker_stop_event = Event()
        self.additional_offset = None
        self.offset_start = None
        self.offset_stop = None
        self.df_values_created = None
        self.plc_ip = None
        self.datablock_number = None
        self.interval_s = None 
        
    def __str__(self):
        info = '''
        This is the Broker class
        It requires a proper plc's db .xlsx config file within the config_file_path
        Before starting a broker, additional functions must be invoked
        Check -> auto_config()
        '''
        return info
        
    def prepare_value_frame(self):
        '''
        Read config file, create new value dataframe
        '''
        self.df_datablock_plc = pd.read_excel('ExchangeData.xlsx', usecols=['Name', 'Data type', 'Offset', 'Comment'])
        self.df_datablock_plc['Value'] = None
        self.df_values = self.df_datablock_plc[['Offset', 'Value', 'Data type', 'Name']].copy().set_index('Offset')
        self.df_values_created = True
        return 'Broker> Value dataframe successfully created'
    
    def compute_additional_offset(self):
        '''
        Depending on the type of the last value in datablock the max byte range is likely to change
        adjusting additional offset prevents the problem
        '''
        assert self.df_values_created == True
        last_offset = self.df_values.iloc[-1]
        last_value_type = last_offset['Data type']
        self.additional_offset = s7_additional_offset[last_value_type]
        return 'Broker> Additional offset added'  

    def define_full_byte_range(self):
        '''
        Define byte range to read, read it all, offset is the index
        '''
        assert not self.additional_offset is None
        self.offset_start = int(self.df_values.index.min())
        self.offset_stop = int(np.ceil(self.df_values.index.max())) + self.additional_offset
        return 'Broker> Full byte range set'
        
    def auto_config(self):
        '''
        Perform auto configuration of essential class parameters
        Manually the functions bellow must be invoked (in the right order):
            - prepare_value_frame()
            - compute_additional_offset()
            - define_full_byte_range()
        '''
        print(self.prepare_value_frame())
        print(self.compute_additional_offset())
        print(self.define_full_byte_range())
    
    def change_connection_options(self, plc_ip:str, datablock_number:int, interval_s:float):
        self.plc_ip = plc_ip
        self.datablock_number = datablock_number
        self.interval_s = interval_s   
        
    def verify_configuration(self):
        assert self.df_values_created == True
        assert not self.offset_start is None
        assert not self.offset_stop is None
        assert not self.additional_offset is None
        assert not self.plc_ip is None
        assert not self.datablock_number is None
        assert not self.interval_s is None
        
    def stop(self):
        '''
        Stop the broker
        '''
        self.broker_stop_event.set()
    
    def connect_PLC(self):
        '''
        Perform initial connection
        '''
        status_connected = False  
        try:
            socket.inet_aton(self.plc_ip)
            self.verify_configuration()
            self.plc_client.connect(self.plc_ip, rack=0, slot=1, tcpport=102)
        except RuntimeError: 
            self.broker_queue.put_nowait('kill consumer')
            print('Broker> Could not perform initial connection, exitting ...')
        except OSError:
            self.broker_queue.put_nowait('kill consumer')
            print('Broker> Wrong ip address, exitting ...')
        except AssertionError:
            self.broker_queue.put_nowait('kill consumer')
            print('Broker> Wrong configuration, exitting ...')
            
        else:
            print('Broker> Connected')
            status_connected = True  
        return status_connected
    
    def reconnect_PLC(self):
        '''
        Function performs max 3 attempts
        Return True if reconnected
        '''
        attempt_count = 1 
        while attempt_count <= 3 and not self.plc_client.get_connected():
            print(f'Broker> Reconnecting ... attempt:{attempt_count}')
            try:
                self.plc_client.connect(self.plc_ip, rack=0, slot=1, tcpport=102)
            except RuntimeError:
                attempt_count += 1
        else: return self.plc_client.get_connected()
         
            
    def run(self):
        '''
            Read plc data until the connection is interrupted,
            dataframe with values filled sent to queue is the result
        '''
        broker_condition_stop = not self.connect_PLC() 

        while not broker_condition_stop and not self.broker_stop_event.is_set():
            try:
                data_plc = self.plc_client.read_area(
                                                    area=snap7.types.Areas.DB,
                                                    dbnumber=self.datablock_number,
                                                    start=self.offset_start,
                                                    size=self.offset_stop
                                                    )                        
            except RuntimeError:
                print('Broker> Cant receive data!')
                # Try to reconnect
                self.broker_queue.put_nowait('Broker attempts to reconnect')
                broker_condition_stop = not self.reconnect_PLC()
                
            else:
                for offset in self.df_values.index:
                    data_type = self.df_values['Data type'].loc[offset]
                    value = s7frame_extract(data_plc, offset, data_type)
                    self.df_values['Value'].loc[offset] = value
                result = self.df_values[['Value','Name']].copy().set_index('Name')
                self.broker_queue.put_nowait(result)
                
            finally:
                time.sleep(self.interval_s)            
        else:
            self.plc_client.disconnect()
            self.broker_queue.put_nowait('kill consumer')
            print('Broker thread is finshed!')
            


            
            
            
