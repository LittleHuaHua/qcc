 To users:

The main usage for module 'QCC' is to extract fundamental and quote data from the QCC database by linking the corresponding api address.

Installing Instruction
To have the access to the module 'QCC' in python, follow the next steps:
1. Download the ".egg" file for the module 
2. Open the terminal and enter the command for easy_install 
	>>> easy_install <filename.egg>
3. Now, in any python shell, import QCC to access the module and functions 

Description: 
1. Class qcc(url, jwt): It takes in two parameters url and jwt, and provides access to functions getFundamentalData() and getQuoteData()
	- url is the base url of the QCC server instance, and 
	- jwt is the token provided to access QCC database

2. getFundamentalData(ID, start, end, mon, day, dow, hours, mins, sec)
	It takes in the parameters: 
	- ID: A string representing the instrumentId desired for user. If missing, error will return. 

	- start: A string, in the form of "yyyy-mm-dd", representing the begin date for the data as desired. 

	- end: A string, in the form of "yyyy-mm-dd", representing the end date for the data as desired. 

	- mon: A string of number, where number is in the range from 01 to 12 representing the month number of date for the data as desired. 01 for Jan, and 12 for Dec. If more than 1 number is desired, separate numbers by "," in a single string (note space is allowed). 

	- day: A string of number, where number is in the range from 01 o 31 representing the day number of date in a month. If more than 1 number, separate them by ",".

	- dow: A string of number, where number is in the range from 00 to 06 representing the day of week. 00 for Sun and 06 for Sat. If more than 1 number, separate them by ",". 

	- hours: A string of number, where number is in the range from 00 o 59 representing the hour in a day. If more than 1 number, separate them by ",".

	- mins: A string of number, where number is in the range from 00 to 59 representing the minutes in an hour. If more than 1 number, separate them by ",".

	- sec: A string of number, where number is in the range from 00 to 59 representing the seconds in a minute. If more than 1 number, separate them by ",".

3. getQuoteData(ID, freq, fields, from, to)
	It takes in the parameters: 
	- ID: A string representing the intrument id desired for user. If missing, error will return. 

	- freq: A string representing the prequence of the desired data. 'freq' can be chosen from '1min', '5min', '15min', 'Day', 'Hour'. 

	- fields: A string containing the fields for different products, such as InstrumentID, ClosePrice. Different fields are separated by space in the string.

	- from: A string of date in the form of "yyyy.mm.dd" representing the start date of the data list desired 

	- to: A string of date in the form of "yyyy.mm.dd" representing the end date of the data list desired 


To use this custom library, in any python shell: 
>>> import QCC
>>> from QCC import qcc
Usage: 
>>> qcc1 = QCC.qcc(url, jwt), where
>>> qcc1.getFundamentalData(ID=, ...)
>>> qcc1.getQuoteData(ID=, ...)