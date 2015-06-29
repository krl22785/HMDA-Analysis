import operator
import sys
from pyspark import SparkContext

def mapper(line):
    if line.startswith('tract_to_msamd_income,rate_spread,population'):
        return ('', (0.00, 1.00))
    fields = eval("(%s)" % line)
    key = (fields[9], fields[17], fields[26], fields[35])
    amount = fields[6]
    income = fields[8]
    if amount == '' or income == '': 
        amount = 0.00
        income = 1.00 
        value = (amount, income)
        return (key, value) 
    else:
        value = (float(amount), float(income))	
        return (key, value)  

if __name__=='__main__':
    if len(sys.argv)!=3:
        print "Usage: DebtIncome <input path> <output path>"
        sys.exit(-1)
    sc = SparkContext(appName="Debt Income Ratio") # do not set 'master'
    output = sc.textFile(sys.argv[1]).map(mapper) #.reduceByKey(lambda a, b: tuple(map(operator.add, a, b)));
    #output.mapValues(lambda x: x[0]/x[1]).saveAsTextFile(sys.argv[2])
    output.saveAsTextFile(sys.argv[2]) 
