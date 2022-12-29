import pandas as pd
import numpy as np
import snap7
import time
import os
from queue import Queue
from threading import Event

def s7clear_logs(path:str):
    try:
        open(path,'w').close()
    except:
        print(f'Could not clear the file on path: {path}')
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


class Broker():

    '''
    Broker class
    It provides a small example of data exchange mechanism between Python and a PLC
    
    Arguments required:
        - config_file_path (the file contains specified TIA portal db interface)
    '''

    def __init__(self, config_file_path:str):
        '''
        Save config file path
        Initialize s7 client
        '''
        self.config_file_path = config_file_path
        self.plc_client = snap7.client.Client()
    
    def prepare_value_frame(self):
        '''
        Read config file, add an empty 'Value' column
        '''
        self.df_datablock_plc = pd.read_excel('ExchangeData.xlsx', usecols=['Name', 'Data type', 'Offset', 'Comment'])
        self.df_datablock_plc['Value'] = None
        self.df_values = self.df_datablock_plc[['Offset', 'Value', 'Data type', 'Name']].copy().set_index('Offset')
        return self.df_values.copy()
    
    def compute_additional_offset(self):
        '''
        Depending on the type of the last value in datablock the max byte range is likely to change
        adjusting additional offset prevents the problem
        '''
        additional_offset = 0
        last_offset = self.df_values.iloc[-1]
        last_value_type = last_offset['Data type']
        
        if last_value_type=='Bool':
            additional_offset = 0
        elif last_value_type=='Int':
            additional_offset = 2
        elif last_value_type=='Real':
            additional_offset = 4

        self.additional_offset = additional_offset    
        return additional_offset

    def define_full_byte_range(self):

        # Define byte range to read, read it all, offset is the index
        self.offset_start = int(self.df_values.index.min())
        self.offset_stop = int(np.ceil(self.df_values.index.max())) + self.additional_offset

    def extract_s7frame_data(self, offset:float, s7frame:bytearray):
        '''
        Extract value from s7frame
        Offset is an indicator for value's data type 
        '''
        bytesToRead = 0
        bitsToRead = 0
        bytes = None
        bit = None
        value = None

        # Separate the number and its floating point
        byteStartIndex = int(offset)
        bitStartIndex = np.ceil((offset*10-byteStartIndex*10)).astype('uint8')
        dataType = self.df_values['Data type'].loc[offset]

        # Change data amount to read according to the s7 data types
        if dataType == 'Int':
            bytesToRead = 2
        elif dataType == 'Real':
            bytesToRead = 4
        elif dataType == 'Bool':
            bitsToRead = 1
        
        # Get binary data and transform it to the actual value
        if bytesToRead>0:
            bytes = s7frame_get_bytes(s7frame, byteStartIndex, byteStartIndex+bytesToRead)
            value = s7buffer_to_value(bytes, dataType)
        elif bitsToRead==1:
            bit = s7frame_get_bit(s7frame, byteStartIndex, bitStartIndex)
            value = bit
            
        return value
    
    def reconnect_PLC(self, plc_ip:str):
        '''
        Function performs max 3 attempts
        Return True if reconnected
        '''
        attempt_count = 1 
        while attempt_count <= 3 and not self.plc_client.get_connected():
            print(f'Broker> Reconnecting ... attempt:{attempt_count}')
            try:
                self.plc_client.connect(plc_ip, rack=0, slot=1, tcpport=102)
            except RuntimeError:
                attempt_count += 1
        else: return self.plc_client.get_connected()
            
            
    def broker_thread(self, plc_ip:str, datablock_number:int, interval_s:float, plc_queue:Queue, break_event:Event, start_logging:bool=False):
        '''
            Read plc data until the connection is interrupted,
            dataframe with values filled sent to queue is the result
        '''
        broker_condition_stop = False
        
        try:
            self.plc_client.connect(plc_ip, rack=0, slot=1, tcpport=102)
        except RuntimeError: 
            plc_queue.put_nowait('kill consumer')
            print('Broker> Could not perform initial connection')
        
        broker_condition_stop = True if not self.plc_client.get_connected() else print('Broker> Connected')
        while not broker_condition_stop and not break_event.is_set():
            try:
                data_plc = self.plc_client.read_area(
                                                    area=snap7.types.Areas.DB,
                                                    dbnumber=datablock_number,
                                                    start=self.offset_start,
                                                    size=self.offset_stop
                                                    )
                if start_logging:
                    try:
                        with open('logs/plc_data.txt', 'a+') as f:
                            f.write(str(data_plc)+'\n')
                    except FileNotFoundError:
                        os.mkdir('logs')
                        
                        
            except RuntimeError:
                print('Broker> Cant receive data!')
                # Try to reconnect
                plc_queue.put_nowait('Broker attempts to reconnect')
                broker_condition_stop = not self.reconnect_PLC(plc_ip)
                
            else:
                for offset in self.df_values.index:
                    value = self.extract_s7frame_data(offset, data_plc)
                    self.df_values['Value'].loc[offset] = value
                result = self.df_values[['Value','Name']].copy().set_index('Name')
                plc_queue.put_nowait(result)
                
            finally:
                time.sleep(interval_s)            
        else:
            self.plc_client.disconnect()
            plc_queue.put_nowait('kill consumer')
            print('Broker thread is finshed!')
            


            
            
            
