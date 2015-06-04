#!/usr/bin/python

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
		print value
		#mortgage_amount = int(value[0])
		#mortgage_count = int(value[1])  		

		#if key == current_key:
		#	print 'ok'
			#current_count += mortgage_count
			#current_sum += mortgage_sum 
		#else:
		#	if current_key:
		#		print "%s\t%d, %d" % (key, current_sum, current_count) 
		#	current_key = key 
			#current_count = int(value[1]) 
			#current_count = int(value[0]) 
		#	print value[0], value[1]	
			
	


if __name__=='__main__':
	reduce()  
