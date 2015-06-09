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

public class AverageMortgage {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text amnt = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.substring(1, line.length()-1).split("\",\"");
            if ((fields.length==47) && fields[19].equals("Home purchase") && fields[46].equals("Loan originated") && fields[20].equals("Secured by a first lien")) {
                word.set(fields[9] + ":" + fields[17] + ":" + fields[26] + ":" + fields[34] + ":" + fields[35]);
                if (fields[6].length()==0)
                    amnt.set("0:1");
                else
                    amnt.set(fields[6] + ":1");
                context.write(word, amnt);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumAmount = 0.0;
            int    sumCount = 0;
            for (Text val : values) {
                String[] numbers = val.toString().split(":");
                sumAmount += Double.parseDouble(numbers[0]);
                sumCount  += Integer.parseInt(numbers[1]);
            }
            result.set(Double.toString(sumAmount) + ":" + Integer.toString(sumCount));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Mortgage");
        job.setJarByClass(AverageMortgage.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(16);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
