import operator 
from pyspark import SparkContext
import StringIO
import csv
import sys 

def mapper(line):
	if line.startswith('tract_to_msamd_income'): 
		return (('', '', '', '', ''), (0.00, 0.00)) 

	fields = eval("(%s)" % line)	
	key = (fields[9], fields[17], fields[26], fields[34], fields[35])	

	value = (float(fields[6]), 1)
	return (key, value) 

if __name__=='__main__': 
	if len(sys.argv) != 3:
		print "Usage: Average Mortgage <input path> <output path>" 
		sys.exit(-1) 
	
	sc = SparkContext(appName = "Average Mortgage") 
	outputFile = sc.textFile(sys.argv[1]).map(mapper).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) 
	outputFile.saveAsTextFile(sys.argv[2])
	#outputFile2 = outputFile.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) 	
	#print output2.collect() 
	
