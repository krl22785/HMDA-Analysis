from pyspark import SparkContext
import sys 

def find_owner_occupancy(att): 
	if att == 'Owner-occupied as a principal dwelling':
		return 0 
	elif att == 'Not owner-occupied as a principal dwelling': 
		return 1 
	else:
		return 2 

def find_loan_purpose(att):
	if att == 'Refinancing':
		return 3
	elif att == 'Home purchase':
		return 4
	else:
		return 5 

def find_gender(att):
	if att == 'Male':
		return 6
	else:
		return 7 

def find_race(att):
	if att == 'White':
		return 8
	elif att == 'Black or African American':
		return 9
	elif att == 'Asian':
		return 10
	else:
		return 11 

def find_loan_origination(att):
	if att == 'Loan originated':
		return 12 
	elif att == 'Application denied by financial institution':
		return 13 
	elif att == 'Loan purchased by the institution':
		return 14
	elif att == 'Application withdrawn by applicant':
		return 15
	elif att == 'Application approved but not accepted':
		return 16
	elif att == 'Preapproval request denied by financial institution':
		return 17
	elif att == 'Preapproval request approved but not accepted':
		return 18 
	else:
		return 19 

def mapper(line):
	if line.startswith('tract_to_msamd_income'):
		return ("none", (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
	else:
		fields = eval(line)

		'''
		Need to add filters so appropriate data is observed.  Mortgage amount and income can 	
		not be blank.  Therefore those observations are omitted from the analysis.  

		features = [
		1. population,
		2. minority population, 
		3. owner occupied units, 
		4. median income,
		5. average application loan amount, 
		6. average applicant income, 
		7. average debt/income ratio, 
		8. count, 
		9. # of 1 to 4 family homes in CT,
		 
		count of owner_occupied as principle dwelling,  owner occupancy name 
		count of non_owner_occupied as principle dwelling, owner occupancy name
		count of na for owner_occupancy_name 
		count of refinancing,   loan purpose name 
		count of home purchase, loan purpose name 
		count of home improvement, loan purpose name
		count of male applications, applicant gender 
		count of female applications, applicant gender 
		count of white applications, applicant race
		count of black applications, applicant race
		count of asian applications, applicant race 
		count of american indian / native america applicants,  applicant race 
		count of native hawaiian applicants, applicant race 
		count of not applicable, applicant race 
		] 
		'''
		
		''' 
		Static Attributes
		'''
	
		if fields[17] != '' and fields[6] != '' and fields[8] != '' and \
			fields[2] != '' and fields[3] != '' and fields[4] != '' and \
			fields[7] != '': 
		
			# Key
			year = fields[35]
			state = fields[10]
			msa = fields[17]
			county = fields[26]
			census_tract = fields[34]
			
			# Static Features; part of Key
			census_tract_pop = float(fields[2]) 	#[float(fields[2]) if fields[2] != '' else 0.00]  
			census_tract_min_pop = float(fields[3]) #[float(fields[3]) if fields[3] != '' else 0.00] 
			owner_occupied = float(fields[4]) 	#[float(fields[4]) if fields[4] != '' else 0.00] 
			msa_median_inc = float(fields[7]) 	#[float(fields[7]) if fields[7] != '' else 0.00]  
				
			# Non-Static Features; part of Value 
			loan_amount = float(fields[6])
			income = float(fields[8])			
			ratio = float(loan_amount) / float(income)
			count = 1 
			one_to_four_family_homes = float(fields[5]) if fields[5] != '' else 0.00
 				
			attributes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
			attributes[find_owner_occupancy(fields[16])] += 1 			
			attributes[find_loan_purpose(fields[19])] += 1
			attributes[find_gender(fields[37])] += 1
			attributes[find_race(fields[42])] += 1
			attributes[find_loan_origination(fields[46])] += 1
			val2 = tuple(attributes) 			

			key = (year, state, msa, county, census_tract, census_tract_pop, \
			census_tract_min_pop, owner_occupied, msa_median_inc)
				
			val1 = (loan_amount, income, ratio, count, one_to_four_family_homes)
			value = val1 + val2 
				
			return (key, value) 

		else:
			return ("none", (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
		
def reducer(a, b):
	return tuple([a[i] + b[i] for i in range(len(a))])

if __name__=='__main__': 

	if len(sys.argv) != 3:
		print "Usage: msaCluster <input path> <output path>" 
		sys.exit(-1)
	
	sc = SparkContext(appName = "MSA Features")
	msa_data = sc.textFile(sys.argv[1]).map(mapper)
	msa_data_grouped = msa_data.reduceByKey(reducer).saveAsTextFile(sys.argv[2]) 

	#file = "../files/hmda_sample.csv"
	#sc = SparkContext("local", "MSA Cluster")

