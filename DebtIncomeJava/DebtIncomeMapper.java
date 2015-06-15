import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DebtIncomeMapper extends Mapper<Object, Text, Text, Text> {
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
