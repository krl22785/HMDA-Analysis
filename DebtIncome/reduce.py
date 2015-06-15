#!/usr/bin/python

import sys

def parseInput(): 
	for line in sys.stdin: 
		yield line.strip('\n').split('\t')

def reduce(): 
	current_key = None 
	current_count = 0 
	current_sum = 0
	current_income = 0 
 
	for key, value in parseInput():
		
#:5,17s/^/#/g 	
		value = eval(value)
		
		try:	
			mortgage_sum = int(value[0])
			income_sum = int(value[1]) 
			mortgage_count = int(value[2])  		
		except:
			mortgage_sum = 0
                        income_sum = 0
                        mortgage_count = 0

		if key == current_key:
			current_sum += mortgage_sum
			current_income += income_sum   
			current_count += mortgage_count		
		else:
			if current_key:
				debtincomeRatio = current_sum/float(current_income)
				print "%s\t%s" % (key, debtincomeRatio)
#
			try:
				current_key = key
				current_sum = mortgage_sum
				current_income = income_sum	
				current_count = mortgage_count 
			except: 
				pass 
							
if __name__=='__main__':
	reduce()  
