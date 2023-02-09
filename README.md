# Data-Acquisition-System-for-PLC-s7

The main goal of the project is to collect specified data from a s7 PLC.<br />
The PLC is controlling three tanks simulated in Factory I/O.<br />
In this particular case s7 communication protocol has been proposed.<br />

# s7Broker

Broker class provides a small functionality to connect to the plc
and read data from a datablock.<br /> Its first and only argument is
a path to a configuration file of non-optimised datablock.<br /> The file must
have the same structure as a datablock visible in TIA Portal.

There is an option to run the samples without any specific hardware,<br />
but there is some software required.<br />
The essential positions are:
- TIA Portal v15.1+
- Factory I/O
- Python v3.7, modules (NumPy, Pandas, openpyxl, snap7, PyQt6, AWSIoTPythonSDK)
- NetToPLCsim

Directory TiaPortalProject contains both plc and factory io files.<br />
The rest of items are used in Python environment.<br />
simple_consumer provides an example of data exchange between a consumer and a PLC.<br />
s7_simulator simulates communication and requires only Python to run it.<br />
aws_iot_publisher describes a mechanism to publish plc data into the AWS cloud.<br />

![](https://github.com/schneiderautomatyka/Data-Acquisition-System-for-PLC-s7/blob/main/Imgs/app.png)

![](https://github.com/schneiderautomatyka/Data-Acquisition-System-for-PLC-s7/blob/main/Imgs/tanks.png)

