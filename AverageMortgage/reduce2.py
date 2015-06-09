#!/usr/bin/python
#comments

import sys

def parseInput():
	for line in sys.stdin:
		yield line.strip('\n').split('\t')

def reduce():
	current_key = None
	current_count = 0
	current_sum = 0
	
	for key, value in parseInput():
		value = eval(value) 
		
		mortgage_sum = int(value[0])
		mortgage_count = int(value[1])
	
		if key == current_key:
			current_sum += mortgage_sum
			current_count += mortgage_count
		else:
			if current_key:
				#print current_sum, current_count 
				#avg_mortgage = float(current_sum)/float(current_count)
				a, b, c, d, e = eval(key)
				print "%s\t%s\t%s\t%s\t%s\t%s\t%s" % (a, b, c, d, e, current_sum, current_count) 
				#print "%s\t%.2f" % (current_key, avg_mortgage)			
	
			current_key = key
			current_sum = int(value[0])
			current_count = int(value[1])

if __name__=='__main__':
	reduce()
