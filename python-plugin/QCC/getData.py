import requests
import json
class qcc: 
	def __init__(self, baseURL, jwt):
		if baseURL[-1] == "/": self.url = baseURL[:-1]
		else: self.url = baseURL
		self.token = jwt

	def getFundamentalData(self, ID=None, start=None, end=None, mon=None, \
						   day=None, dow=None, hours=None, mins=None, sec=None):
		if ID is None: 
			print("Error: no product id specified.")
			return 

		url = self.url + "/api/classifications?where={dataName:'" + ID + "'}" + "&jwt=" + self.token
		response = requests.get(url)
		info = response.json()
		info = dict(info[0])
		classification = info['apiAddress']
		productType = info['fields']
		if len(info) == 0: 
			print("Error: no matching id")
			return 

		if start is None: start = ""
		else: start = "&from=" + start

		if end is None: end = ""
		else: end = "&to=" + end

		if mon is None: mon = ""
		else: mon = "&mon=" + mon

		if day is None: day = ""
		else: day = "&day=" + day

		if dow is None: dow = ""
		else: dow = "&dow=" + dow

		if hours is None: hours = ""
		else: hours = "&hour=" + hours

		if mins is None: mins = ""
		else: mins = "&min=" + mins

		if sec is None: sec = ""
		else: sec = "&sec=" + sec

		url2 = self.url + classification + "?jwt=" + self.token+ start + end + mon + day + dow + hours + mins + sec
		print(url2)
		response = requests.get(url2)
		data = response.json()
		n = len(data)
		if n == 0: return data

		if 'dataId' in productType:
			delete_entry = 'dataId'
		else: delete_entry = 'instrumentId'		

		for i in range(n):
			d_i = data[i]
			del d_i['id']
			del d_i[delete_entry]
			data[i] = d_i
			i += 1

		return data


	def getQuoteData(self, ID=None, freq=None, fields=None, start=None, end=None):
		if ID is None: 
			print("Error: no product specified.")
			return 
			
		if freq is None: 
			freq = ""
		else: freq = "/" + freq

		if fields is None: 
			fields = ""
		else: fields = "&fields=" + fields

		if start is None: 
			start = ""
		else: start = "&from=" + start

		if end is None: 
			end = ""
		else: end = "&to=" + end 

		url = self.url + ID + freq + self.token + fields + start + end
		print(url)
		response = requests.get(url)
		data = response.json()

		return data
	
# qcc1 = qcc("http://analytics.qcc-x.com", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6MSwiaWF0IjoxNDYyOTk3ODY3fQ.J9nVYvU3KRrX4dUao5f47nJo5roAYnwZ2UbOaY5mIlI")
# qcc1.getFundamentalData(ID="I1703.DCE")