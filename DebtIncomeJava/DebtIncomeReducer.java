import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DebtIncomeReducer 
	extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text(); 
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double debtAmount = 0.0; 
		double incomeAmount = 0.0;
		double ratio; 
		 
		int sumCount = 0; 
		
		for (Text val: values) {
			String[] numbers = val.toString().split(":");
			debtAmount += Double.parseDouble(numbers[0]);
			incomeAmount += Double.parseDouble(numbers[1]); 
			sumCount += Integer.parseInt(numbers[2]); 
			}
		
		ratio = debtAmount/incomeAmount;		
		result.set(Double.toString(ratio));
		context.write(key, result); 
		}	
	}	
