import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> { 
	private static final int MISSING = 9999;
	
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
		
		String year; 
		int airTemperature; 

		String line = value.toString();
		String[] fields = line.split(",");
		year = fields[1];
		airTemperature = fields[2]; 
		
		if (year.length() == 8) {
			context.write(new Text(year), new IntWritable(airTemperature));  
		}
	}			
}
