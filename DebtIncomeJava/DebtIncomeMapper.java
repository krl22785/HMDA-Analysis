import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DebtIncomeMapper extends Mapper<Object, Text, Text, Text> { 
	private Text keycombo= new Text();
	private Text valuecombo = new Text();
	
	public void map(Object key, Text, value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); 
		String[] fields = line.substring(1, line.length()-1).split("\",\"");
		if ((fields.length() == 47) && fields[14].equal("One-to-four family dwelling (other than manufactured housing)") && fields[19].equal("Home purchase") && 
			fields[20].equal("Secured by a first lien") && fields[46].equal("Loan originated")) { 
				keycombo.set(fields[9] + ":" + fields[17] + ":" + fields[26] + ":" + fields[35]);

				if (fields[6].length() == 0) 
					valuecombo.set("0:1"); 
				else
					valuecombo.set(fields[6] + ":" + fields[8] + ":1"); 	
			context.write(keycombo, valuecombo) 
			}
		}
	
	}
