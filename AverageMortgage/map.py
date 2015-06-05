#!/usr/bin/python

import sys
import os
import StringIO
import csv 

def parseInput():
	for line in sys.stdin:
		csv_file = StringIO.StringIO(line) 
		csv_reader = csv.reader(csv_file) 
			
		for record in csv_reader:
			yield record 

def mapper():
	for record in parseInput(): 

		if len(record) == 47:	
			state = record[9] 
			msa = record[17]
			county = record[26] 
			census_tract = record[34]
			year = record[35]
			
			loan_purpose = record[19] 
			action_taken = record[46]
			
			lien_status = record[20] 

			if loan_purpose == 'Home purchase' and action_taken == 'Loan originated' and lien_status == 'Secured by a first lien':   
				key = (state, msa, county, census_tract, year) 	
				amount = record[6]
				value = (amount, 1) 
				print "%s\t%s" % (key,value)  
			else:
				pass 
		else:
			pass 		

if __name__=='__main__':
	mapper() 
