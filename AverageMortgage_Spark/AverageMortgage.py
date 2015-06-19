"""SimpleApp.py"""
from pyspark import SparkContext
import StringIO
import csv

def loadRecord(line):
	input = StringIO.StringIO(line) 
	reader = csv.reader(input) 
	return reader.next() 

def mapper(line):
	key = (line[9], line[17], line[26], line[34], line[35])
	value = (float(line[6]), 1)
	return (key, value) 

if __name__=='__main__': 

	file = "../spark/hmda_sample3.csv"
	sc = SparkContext("local", "Average Mortgage")

	logData = sc.textFile(file).map(loadRecord).cache()
	outputMap = logData.map(mapper) 
	
	output = outputMap.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) 	
	print output.collect() 
	
