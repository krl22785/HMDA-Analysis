import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DebtIncome {

	public static class DebtIncomeMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
                private Text amnt = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String line = value.toString();
                        String[] fields = line.substring(1, line.length()-1).split("\",\"");
                        if ((fields.length==47) && fields[14].equals("One-to-four family dwelling (other than manufactured housing)") && fields[19].equals("Home purchase") && fields[20].equals("Secured by a first lien") && fields[46].equals("Loan originated") && !fields[17].equals("")) {
                                word.set(fields[9] + ":" + fields[17] + ":" + fields[26] + ":" + fields[35]);
                        if (fields[6].length() == 0 || fields[8].length() == 0)
                                amnt.set("-1.0:-1.0:1");
                        else
                                amnt.set(fields[6] + ":" + fields[8] + ":1");
                context.write(word, amnt);
                        }
                }
        }

	
	public static class DebtIncomeReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			double debtAmount = 0.0; 
			double incomeAmount = 0.0; 
                	double ratio; int sumCount = 0; 
			for (Text val: values) { 
			String[] numbers = val.toString().split(":");
			debtAmount += Double.parseDouble(numbers[0]);
			incomeAmount += Double.parseDouble(numbers[1]);  
			sumCount += Integer.parseInt(numbers[2]);  
				}
				ratio = debtAmount/incomeAmount; 
				//result.set(Double.toString(debtAmount) + ":" + Double.toString(incomeAmount) + ":" + Integer.toString(sumCount)); 	
				result.set(Double.toString(ratio)); 
				context.write(key, result);
			}
		}

 
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Debt Income"); 
		job.setJarByClass(DebtIncome.class);
		job.setMapperClass(DebtIncomeMapper.class);
		job.setReducerClass(DebtIncomeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
