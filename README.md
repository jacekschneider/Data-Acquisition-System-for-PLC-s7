# Data-Acquisition-System-for-PLC-s7

The main goal of the project is to collect specified data from s7 PLCs.
The PLC is controlling three tanks simulated in Factory I/O.
In this particular case s7 communication protocol has been proposed.

# s7broker

s7broker class provides a small functionality to connect to the plc
and read data from a datablock. Its first and only argument is
a path to a configuration file of non-optimised datablock. The file must
have the same structure as a datablock visible in TIA Portal.

There is an option to run s7broker and the whole project without any specific hardware,
but there is some software required.
The essential positions are:
- TIA Portal v15.1+
- Factory I/O
- Python v3.8+, modules (NumPy, Pandas, snap7)
- NetToPLCsim

Directory TiaPortalProject contains both plc and factory io files.
The rest of items are used in Python environment.
s7Broker_example.py provides an example of data exchange between a consumer and a PLC.
s7BrokerSim_example.py simulates communication, but requires only Python to run it.




