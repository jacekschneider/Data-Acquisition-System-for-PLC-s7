import pandas as pd
import numpy as np
import snap7

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

        # Depending on the type of the last value in db the max byte range is likely to change
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

        # Define byte range to read, read it all
        self.offset_start = int(self.df_values['Offset'].min())
        self.offset_stop = int(np.ceil(self.df_values['Offset'].max())) + self.additionalOffset

    def s7frame_get_byte(self, buffer:bytearray, index:int):
        return buffer[index]

    def s7frame_get_bytes(s7frame:bytearray, indexStart:int, indexStop:int):
        return s7frame[indexStart:indexStop]

    def s7frame_get_bit(s7frame:bytearray, indexByte:int, indexBit:int):
        byte = np.array(s7frame[indexByte], dtype='uint8')
        bits = np.unpackbits(byte)
        bit = bits[indexBit] if 0<=indexBit<=7 else None
        return bit

    def s7buffer_to_value(buffer, type:str):
        value = None
        if type=='Int':
            value = np.frombuffer(buffer, dtype='>i2')
        elif type=='Real':
            value = np.frombuffer(buffer, dtype='>f')
        
        return value[0]

    def extract_s7frame_data(self, offset:float, s7frame:bytearray):

        bytesToRead = 0
        bitsToRead = 0
        bytes = None
        bit = None
        value = None

        byteStartIndex = int(offset)
        bitStartIndex = np.ceil((offset*10-byteStartIndex*10)).astype('uint8')
        dataType = self.df_values['Data type'].loc[offset]

        if dataType == 'Int':
            bytesToRead = 2
        elif dataType == 'Real':
            bytesToRead = 4
        elif dataType == 'Bool':
            bitsToRead = 1
        
        if bytesToRead>0:
            bytes = self.s7frame_get_bytes(s7frame, byteStartIndex, byteStartIndex+bytesToRead)
            value = self.s7buffer_to_value(bytes, dataType)
        elif bitsToRead==1:
            bit = self.s7frame_get_bit(s7frame, byteStartIndex, bitStartIndex)
            value = bit

    def broker_thread(self, plc_ip:str, datablock_number:int):

        broker_condition_stop = False
        self.plc_client.connect(plc_ip, rack=0, slot=1, tcpport=102)

        while not broker_condition_stop:

            try:
                data_plc = self.plc_client.read_area(
                                                    area=snap7.types.Areas.DB,
                                                    dbnumber=datablock_number,
                                                    start=self.offset_start, size=self.offset_stop
                                                    )
            except:
                broker_condition_stop = True
            finally:
                for offset in self.df_values.index:
                    value = self.extract_s7frame_data(offset, data_plc)
                    self.dfValues['Value'].loc[offset] = value
            
            result = self.dfValues[['Value','Name']].copy().set_index('Name')
            print(result)
        else:
            print('Broker thread is fnished!')