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
		print key, value 
		
#:5,17s/^/#/g 	
#		value = eval(value)
#		
#		try:	
#			mortgage_sum = int(value[0])
#			mortgage_count = int(value[1])  		
#		except:
#			pass
#
#		if key == current_key:
#			current_sum += mortgage_sum 
#			current_count += mortgage_count		
#		else:
#			if current_key:
#				#avg_mortgage = float(current_sum)/float(current_count) 
#				#print avg_mortgage
#				#print "%s\t%.2f" % (current_key, avg_mortgage) 
#				output_value = (current_sum, current_count) 
#				print "%s\t%s" % (current_key, output_value) 
#
#			try: 
#				current_key = key 
#				current_sum = int(value[0])
#				current_count = int(value[1]) 
#			except:
#				pass 
							
if __name__=='__main__':
	reduce()  
