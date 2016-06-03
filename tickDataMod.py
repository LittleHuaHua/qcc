import csv
import os 
#import time 

def doDay(year, category): 
	'''main function'''
	# start = time.clock()
	basePath = '/home/qcc/Desktop'
	catLayer = basePath + '/tick-data' + '/' + year + '/' + category #go to each file in '/home/qcc/Desktop/tick-data/tick_2010/dc'
	for folder in os.listdir(catLayer):
		oldPath = catLayer + '/' + folder
		get_file(basePath, oldPath)
	# end = time.clock()
	# time_cost = end - start 
	# print(time_cost)
	return 
	
	
def get_file(base_path, old_path): 
	'''fetch the originial csv file'''
	for file in os.listdir(old_path): #go to file: 'a1001_20100104.csv'
		split_name = os.path.splitext(file)
		file_name = str.split(split_name[0], '_')
		contract = file_name[0] #i.e. a1001			
		new_dir = base_path + '/tick-data-mod' + '/' + contract
		old_dir = old_path + '/' + file
		tickDataMod(file, new_dir, old_dir) #i.e, ('a1001_20100104.csv', '/home/qcc/Desktop/tick-data-mod/a1001')
	return 

# def rm_trailingZero(entry):
# 	'''helper function to remove trailing zeroes'''
# 	number = '{0:g}'.format(float(entry))
# 	return number 

def tickDataMod(file, newDir, oldDir): #file: 'a1001_20100104.csv'
	'''for each csv file, add Eng title , remove trailing zeroes and write the new csv file into desired directory'''
	if not os.path.exists(newDir):
		os.makedirs(newDir)

	with open(newDir + '/' + file, 'w', newline='') as f: #path for writing 
		writer = csv.writer(f)
		writer.writerow(["futureId", "date", "latestPrice", "openInterest", "turnover", "volume", "openPosition", \
			"closePosition", "bidPrice", "askedPrice", "bidVolume", "askedVolume"])
		with open(oldDir, encoding='gbk') as original:
			content = csv.reader(original)
			indicies = [0,5,10,11]
			for row in content:
				if (row[0] == 'dc' or row[0] == 'sc' or row[0] == 'zc'): 
					modified_row = [i for j, i in enumerate(row) if j not in indicies] #finish reading 1 line
					# remove any trailing zeroes for each entry except for the id and date 
					modified_row[2:] = map(lambda x: '{0:g}'.format(float(x)), modified_row[2:]) 
					writer.writerow(modified_row)
	return

# def tickDataMod(file, newDir, oldDir): #file: 'a1001_20100104.csv'
# 	if not os.path.exists(newDir):
# 		os.makedirs(newDir)

# 	with open(newDir + '/' + file, 'w', newline='') as f: #path for writing 
# 		writer = csv.writer(f)
# 		writer.writerow(["futureId", "date", "latestPrice", "openInterest", "turnover", "volume", "openPosition", \
# 			"closePosition", "bidPrice", "askedPrice", "bidVolume", "askedVolume"])
# 		with open(oldDir, encoding='gbk') as original:
# 			content = csv.reader(original)
# 			indicies = [0,5,10,11]
# 			for row in content:
# 				if (row[0] == 'dc' or row[0] == 'sc' or row[0] == 'zc'): 
# 					modified_row = [i for j, i in enumerate(row) if j not in indicies] #finish reading 1 line
# 					for entry in modified_row:
# 						j = modified_row.index(entry)
# 						if j >= 2: 
# 							modified_row[j] = '{0:g}'.format(float(entry))
# 					writer.writerow(modified_row)
# 	return

#Execution
# doDay('tick_2010', 'dc')
# doDay('tick_2010', 'sc')
# doDay('tick_2010', 'zc')
# doDay('tick_2011', 'dc')
# doDay('tick_2011', 'sc')
# doDay('tick_2011', 'zc')
# doDay('tick_2012', 'dc')
# doDay('tick_2012', 'sc')
# doDay('tick_2012', 'zc')
# doDay('tick_2013', 'dc')
# doDay('tick_2013', 'sc')
# doDay('tick_2013', 'zc')
# doDay('tick_2014', 'dc')
# doDay('tick_2014', 'sc')
# doDay('tick_2014', 'zc')
# doDay('tick_2015', 'dc')
# doDay('tick_2015', 'sc')
# doDay('tick_2015', 'zc')