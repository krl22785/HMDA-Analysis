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
		print record[0:8]

if __name__=='__main__':
	mapper()  
