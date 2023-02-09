import pandas as pd
import numpy as np
import snap7
import time
import socket
from queue import Queue, Full
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

def clear_logs(path:str) -> None:
    '''Clear all the data stored in the path.
    
    Parameters
    ----------
    path : str
        Path to a file.
    '''
    
    try:
        open(path,'w').close()
    except:
        print(f'Could not clear a file on path: {path}')
        
def get_byte(s7frame:bytearray, index:int) -> bytearray:
    '''Get a byte from the s7frame.
    
    Parameters
    ----------
    s7frame : bytearray
        S7 protocol frame.
    index : int
        Byte index.
    
    Returns
    -------
    bytearray
        A single byte within a bytearray.
    '''
    
    return s7frame[index]

def get_bytes(s7frame:bytearray, index_start:int, index_stop:int) -> bytearray:
    '''Get bytes from the s7frame.
    
    Parameters
    ----------
    s7frame : bytearray
        S7 protocol frame.
    index_start : int
        Range start index.
    index_stop : int
        Range stop index.
    
    Returns
    -------
    bytearray
        A byte range within a bytearray.
    '''
    
    return s7frame[index_start:index_stop]

def get_bit(s7frame:bytearray, index_byte:int, index_bit:int):
    '''Unpack a byte and get specified bit.
    
    Parameters
    ----------
    s7frame : bytearray
        S7 protocol frame.
    index_byte : int
        Index of a byte holding a bit value of interest.
    index_bit : int
        Index of a bit in the byte.
    
    Returns
    -------
    np.uint8
        Value of the bit if the index_bit is valid (0-7).
    None
        Returned if the index_bit is invalid.
    '''
    
    byte = np.array(s7frame[index_byte], dtype='uint8')
    bits = np.unpackbits(byte)
    bit = bits[index_bit] if 0<=index_bit<=7 else None
    return bit

def frombuffer(buffer:bytearray, type:str):
    '''Transform bytes to a value depending on the type.\n
    S7 family defines big-endian coding.
    
    Parameters
    ----------
    buffer : bytearray
        Bytes to be transformed into a value.
    type : str
        Type of the value.
    
    Returns
    -------
    float
        If the type was defined as a Real. Real refers to s7 family.
    int
        If the type was defines as an Int.
    None
        If the type differs from the ones above.
    '''
    
    value = None
    if type=='Int':
        value = np.frombuffer(buffer, dtype='>i2')
    elif type=='Real':
        value = np.frombuffer(buffer, dtype='>f')
    return value[0] if not value is None else None

def extract(s7frame:bytearray, offset:float, type:str):
    '''Extract value from s7frame
    
    Parameters
    ----------
    s7frame : bytearray
        S7 protocol frame.
    offset : float
        Offset is an indicator for a postion of a value to be extracted.
    type : str
        Type of the value.
    
    Returns
    -------
    float
        If the type was float
    int
        If the type was an int
    np.uint8
        If the type was bool
    None
        If the type was none of the above
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
    bytes_to_read = s7_bytes_to_read[type]
    bits_to_read = s7_bits_to_read[type]
    
    # Get binary data and transform it to the actual value
    if bytes_to_read>0:
        bytes = get_bytes(s7frame, byte_start_index, byte_start_index + bytes_to_read)
        value = frombuffer(bytes, type)
    elif bits_to_read==1:
        bit = get_bit(s7frame, byte_start_index, bit_start_index)
        value = bit      
    return value


class Broker(Thread):

    '''Broker class\n
    Provides Small example of data exchange mechanism between Python and a PLC.
    
    Parameters
    ----------
    config_file_path : str
        A path to the s7 plc data block configuration file in .xlsx format.
        
    Attributes
    ----------
    config_file_path : str
        A path to the s7 plc data block configuration file in .xlsx format.
    plc_client : snap7.client.Client
        S7 protocol client.
    broker_queue : queue.Queue
        Queue to send over messages.
    broker_stop_event : threading.Event
        Event to stop the broker.
    additional_offset : int or None
        Extra offset value.
    offset_start : int or None
        Offset start index.
    offset_stop : int or None
        Offset stop index.
    df_values_created : bool or None
        True if the df has been created.
    plc_ip : str or None
        Plc's ip.
    datablock_number : int or None
        DB's number.
    interval_s : int or None
        Update time interval in seconds.
    '''

    def __init__(self, config_file_path:str, *args, **kwargs):
        '''
        Save config file path.\n
        Initialize s7 client.
        '''
        super().__init__(*args, **kwargs)
        self.config_file_path = config_file_path
        self.plc_client = snap7.client.Client()
        self.broker_queue = Queue(1)
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
        Read config file, create new value dataframe.
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
        
    def verify_config_params(self):
        assert self.df_values_created == True
        assert not self.offset_start is None
        assert not self.offset_stop is None
        assert not self.additional_offset is None
        
    def verify_communication_params(self):
        assert not self.plc_ip is None
        assert not self.datablock_number is None
        assert not self.interval_s is None
        
    def verify_configuration(self):
        self.verify_config_params()
        self.verify_communication_params()
    
    def get_values(self):
        self.verify_config_params()
        return self.df_values[['Value','Name']].copy().set_index('Name')
        
    def log(self, plc_data:bytearray, path:str='plc_data.txt'):
        with open(path, 'a+') as f:
            f.writelines((' '.join(str(byte) for byte in list(plc_data))) + '\n')   
        
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
                time.sleep(2)
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
                plc_data = self.plc_client.read_area(
                                                    area=snap7.types.Areas.DB,
                                                    dbnumber=self.datablock_number,
                                                    start=self.offset_start,
                                                    size=self.offset_stop
                                                    )
                # Uncomment bellow to log the data
                # self.log(plc_data, 'logs/plc_data.txt')             
            except RuntimeError:
                print('Broker> Cant receive data!')
                self.plc_client.disconnect()
                # Try to reconnect
                broker_condition_stop = not self.reconnect_PLC()
                
            else:
                for offset in self.df_values.index:
                    data_type = self.df_values['Data type'].loc[offset]
                    value = extract(plc_data, offset, data_type)
                    self.df_values['Value'].loc[offset] = value
                result = self.df_values[['Value','Name']].copy().set_index('Name')
                try:
                    self.broker_queue.put_nowait(result)
                except Full:
                    self.broker_queue.get_nowait()
                    self.broker_queue.put_nowait(result)
                
            finally:
                time.sleep(self.interval_s)            
        else:
            self.plc_client.disconnect()
            try:
                self.broker_queue.put_nowait('kill consumer')
            except Full:
                self.broker_queue.get_nowait()
                self.broker_queue.put_nowait('kill consumer')
            print('Broker thread is finshed!')
            

class BrokerSim(Broker):
    '''Inherits from s7comm.Broker class.\n
    It simulates communication and requires only Python to run it.
    
    Parameters
    ----------
    logs_path : str
        A path to a file containing logged s7 frames.
    config_file_path : str
        A path to the s7 plc data block configuration file in .xlsx format.
    '''
    def __init__(self, logs_path:str, config_file_path:str, *args, **kwargs):
        super().__init__(config_file_path, *args, **kwargs)
        self.logs_path = logs_path
            
    def run(self):   
        try:
            self.verify_config_params()
            with open(self.logs_path, 'r') as log_file:
                for line in log_file:
                    if self.broker_stop_event.is_set() : break
                    # Convert a single line into the actual s7frame
                    plc_data = bytearray(map(int, line[:-1].split(' ')))
                    for offset in self.df_values.index:
                        data_type = self.df_values['Data type'].loc[offset]
                        value = extract(plc_data, offset, data_type)
                        self.df_values['Value'].loc[offset] = value
                    result = self.df_values[['Value','Name']].copy().set_index('Name')
                    try: self.broker_queue.put_nowait(result)
                    except Full:
                        self.broker_queue.get_nowait()
                        self.broker_queue.put_nowait(result)
                    time.sleep(1)
            try: self.broker_queue.put_nowait('kill consumer')
            except Full:
                    self.broker_queue.get_nowait()
                    self.broker_queue.put_nowait(result)
            print('BrokerSim> Simulation is finished') 
            
        except FileNotFoundError:
            print(f'BrokerSim> Could not find the file on path: {self.logs_path}')             
        except AssertionError:
            print(f'BrokerSim> Wrong configuration')
            
            
