#!/usr/bin/python

import sys
import os
import StringIO
import csv 

def parseInput():
	for line in sys.stdin:
		yield line.strip('\n').split("\t") 	
		
def mapper():
	for record in parseInput(): 
		print "%s\t%s" % (record[0], record[1])  

if __name__=='__main__':
	mapper() 
