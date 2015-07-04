from pyspark import SparkContext

def mapper(line):
	if line.startswith('tract_to_msamd_income'):
		return ("none", (-1, -1, -1, -1))
	else:
		fields = eval(line)

		'''
		Need to add filters so appropriate data is observed.  Mortgage amount and income can 	
		not be blank.  Therefore those observations are omitted from the analysis.  
		'''
		 
		if fields[14] == "One-to-four family dwelling (other than manufactured housing)" and \
			fields[16] == "Owner-occupied as a principal dwelling" and \
			(fields[46] == "Loan originated" or fields[46] == "Loan purchased by the institution") and \
			fields[17] != '' and fields[6] != '' and fields[8] != '': 

			year = fields[35]
			state = fields[10] 	
			msa = fields[17] 
			county = fields[26]
			census_tract = fields[34]
			census_tract_pop = fields[2]
			census_tract_min_pop = fields[3] 
			owner_occupied = fields[4] 
			msa_median_inc = fields[7] 
			
			key = (year, state, msa, county, census_tract, census_tract_pop, \
			census_tract_min_pop, owner_occupied, msa_median_inc) 
		
			loan_amount = float(fields[6]) 
			income = float(fields[8])
			ratio = float(loan_amount) / float(income)
			
			count = 1	
			value = (loan_amount, income, ratio, count) 
				
			return (key, value) 

		else:
			return ("none", (-1, -1, -1, -1))
		

if __name__=='__main__': 

	if len(sys.argv) != 3: 
		print "Usage: msaCluster <input path> <output path>" 
		sys.exit(-1) 
	
	sc = SparkContext(appName = "MSA Features") 
	msa_data = sc.textFile(sys.argv[1]).map(mapper)	
	msa_data_grouped = msa_data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3]))
	

	#file = "hmda_sample.csv"
	#sc = SparkContext("local", "MSA Cluster")

	#data = sc.textFile(file).map(mapper)	
	
	#data_output = data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3]))

	#for i, j in enumerate(data_output.collect()): print i, j 
	
	#for i in logData.collect():
	#	print i 
	
	#outputMap = logData.map(mapper) 
	
	#output = outputMap.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) 	
	#print output.collect() 
	
