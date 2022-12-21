import pandas as pd
import numpy as np
import snap7 as s7
import time

# dataPLC = bytearray(b'\x00!\x00B27A(\x00\x00C\xa6\xaa\xa0B\xc8\xe6f\x11\x00\x00\x00\x00\x00\x00\x1d\x00\x00\x00\x00\x00\x00\x00\x00A\xf2ff\x00\x00\x00\x00\x10\x00\x00\x00\x00\x1c\x00\x00A\xf4Q\xec\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02')


def s7FrameGetByte(frame:bytearray, index:int):
    return frame[index]

def s7FrameGetBytes(frame:bytearray, indexStart:int, indexStop:int):
    return frame[indexStart:indexStop]

def s7FrameGetBit(frame:bytearray, indexByte:int, indexBit:int):
    byte = np.array(frame[indexByte], dtype='uint8')
    bits = np.unpackbits(byte)
    bit = bits[indexBit] if 0<=indexBit<=7 else None
    return bit

def s7BufferToValue(buffer, type:str):
    value = None
    if type=='Int':
        value = np.frombuffer(buffer, dtype='>i2')
    elif type=='Real':
        value = np.frombuffer(buffer, dtype='>f')
    
    return value[0]

def s7AdditionalOffset(type:str)->int:
    additionalOffset = 0
    if type=='Bool':
        additionalOffset = 0
    elif type=='Int':
        additionalOffset = 2
    elif type=='Real':
        additionalOffset = 4
        
    return additionalOffset
        
        



PLC_IP = '192.168.33.6'
DB_NUMBER = 1

dfDataBlockPLC = pd.read_excel('ExchangeData.xlsx', usecols=['Name', 'Data type', 'Offset', 'Comment'])

# Add a an empty 'Value' column
dfDataBlockPLC['Value'] = None

# Prepare 'Values' data frame, offset is now the index
dfValues = dfDataBlockPLC[['Offset', 'Value', 'Data type', 'Name']].copy().set_index('Offset')

# Depending on the type of the last value in db the max byte range is likely to change
# additional offset prevents the problem
lastOffset = dfValues.iloc[-1]
lastValueType = lastOffset['Data type']
additionalOffset = s7AdditionalOffset(lastValueType)

# Define byte range to read, read it all
offsetStart = int(dfDataBlockPLC['Offset'].min())
offsetNumberOfBytes = int(np.ceil(dfDataBlockPLC['Offset'].max())) + additionalOffset

# Prepare plc client
PLC = s7.client.Client()
PLC.connect(PLC_IP, rack=0, slot=1, tcpport=102)

clock = time.time()
endThreadCondition=False;
while not endThreadCondition:
    
    dataPLC = PLC.read_area(area=s7.types.Areas.DB, dbnumber=DB_NUMBER, start=offsetStart, size=offsetNumberOfBytes)
    
    for offset in dfValues.index:
        
        bytesToRead = 0
        bitsToRead = 0
        bytes = None
        bit = None
        value = None
        byteStartIndex = int(offset)
        bitStartIndex = np.ceil((offset*10-byteStartIndex*10)).astype('uint8')
        dataType = dfValues['Data type'].loc[offset]
        
        if dataType == 'Int':
            bytesToRead = 2
        elif dataType == 'Real':
            bytesToRead = 4
        elif dataType == 'Bool':
            bitsToRead = 1
        
        if bytesToRead>0:
            bytes = s7FrameGetBytes(dataPLC, byteStartIndex, byteStartIndex+bytesToRead)
            value = s7BufferToValue(bytes, dataType)
        elif bitsToRead==1:
            bit = s7FrameGetBit(dataPLC, byteStartIndex, bitStartIndex)
            value = bit
        
        dfValues['Value'].loc[offset] = value
        

    result = dfValues[['Value','Name']].copy().set_index('Name')
    print(result)
    
    endThreadCondition = time.time()-clock>10
    time.sleep(1)

else:
    print('Producer thread is closed!')

PLC.disconnect()




