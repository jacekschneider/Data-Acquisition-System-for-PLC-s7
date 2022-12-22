import pandas as pd
import numpy as np
import snap7
import time
from queue import Queue


def s7frame_get_byte(buffer:bytearray, index:int):
    return buffer[index]

def s7frame_get_bytes(s7frame:bytearray, indexStart:int, indexStop:int):
    return s7frame[indexStart:indexStop]

def s7frame_get_bit(s7frame:bytearray, indexByte:int, indexBit:int):
    
    # Unpack byte and get bit
    byte = np.array(s7frame[indexByte], dtype='uint8')
    bits = np.unpackbits(byte)
    bit = bits[indexBit] if 0<=indexBit<=7 else None
    return bit

def s7buffer_to_value(buffer, type:str):
    value = None
    
    # s7 defines big-endian coding
    if type=='Int':
        value = np.frombuffer(buffer, dtype='>i2')
    elif type=='Real':
        value = np.frombuffer(buffer, dtype='>f')
    return value[0]


class Broker():

    def __init__(self, config_file_path:str):
        self.config_file_path = config_file_path
        self.plc_client = snap7.client.Client()
    
    def prepare_value_frame(self):

        # Read config file, add an empty 'Value' column
        self.df_datablock_plc = pd.read_excel('ExchangeData.xlsx', usecols=['Name', 'Data type', 'Offset', 'Comment'])
        self.df_datablock_plc['Value'] = None
        self.df_values = self.df_datablock_plc[['Offset', 'Value', 'Data type', 'Name']].copy().set_index('Offset')
        return self.df_values.copy()
    
    def compute_additional_offset(self):
        additional_offset = 0

        # Depending on the type of the last value in datablock the max byte range is likely to change
        # adjusting additional offset prevents the problem
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
        bytesToRead = 0
        bitsToRead = 0
        bytes = None
        bit = None
        value = None

        # Separate the number and its floating point
        byteStartIndex = int(offset)
        bitStartIndex = np.ceil((offset*10-byteStartIndex*10)).astype('uint8')
        dataType = self.df_values['Data type'].loc[offset]

        # Change binary data amount to read according to the s7 data types
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

    def broker_thread(self, plc_ip:str, datablock_number:int, interval_s:float, plc_queue:Queue):
        broker_condition_stop = False
        self.plc_client.connect(plc_ip, rack=0, slot=1, tcpport=102)
        broker_condition_stop = True if not self.plc_client.get_connected() else print('PLC> Connected')
        '''
            Read plc data until the connection is interrupted
            dataframe with empty values filled is the result
        '''
        while not broker_condition_stop:
            try:
                data_plc = self.plc_client.read_area(
                                                    area=snap7.types.Areas.DB,
                                                    dbnumber=datablock_number,
                                                    start=self.offset_start,
                                                    size=self.offset_stop
                                                    )
            except RuntimeError:
                broker_condition_stop = True
                print('Broker> Cant receive data!')
            finally:
                for offset in self.df_values.index:
                    value = self.extract_s7frame_data(offset, data_plc)
                    self.df_values['Value'].loc[offset] = value
            
            result = self.df_values[['Value','Name']].copy().set_index('Name')
            plc_queue.put_nowait(result)
            time.sleep(interval_s)            
        else:
            print('Broker thread is finshed!')
            self.plc_client.disconnect()
            print('PLC> Disconnected')
            
            
