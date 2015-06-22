"""DebtIncome.py"""
from pyspark import SparkContext
import StringIO
import csv

# the csv file needs to be processed to remove Non-ASCII characters as Spark will break if that is not done

def loadRecord(line):
        input = StringIO.StringIO(line) 
        reader = csv.reader(input) 
	return reader.next() 

def mapper(line):
	key = (line[9], line[17], line[26], line[35])
	amount = line[6]
	income = line[8]
	if amount == '' or income == '': 
		amount = 0.00
		income = 1.00 
		value = (amount, income) 
		return (key, value) 
	else:
		value = (float(amount), float(income))	
		return (key, value)  


if __name__=='__main__': 
	
	file = "../../spark/hmda_sample3.csv"
	sc = SparkContext("local", "Debt Income Ratio")

	input = sc.textFile(file).map(loadRecord).cache()
	outputMap = input.map(mapper) 
	
	output = outputMap.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) 	
	
	print output.mapValues(lambda x: x[0]/x[1]).collect()
