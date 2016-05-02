package hadoop001;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text d = new Text();
	private Text p = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String date = null;
		String prod = null;

		if (line.charAt(6) == '\t') {
			date = line.substring(0, 6);
			prod = line.substring(7);
		}

		if (line.charAt(7) == '\t') {
			date = line.substring(0, 7);
			prod = line.substring(8);
		}
	
		if (date != null && prod != null) {	
			d.set(date);
			p.set(prod);
			context.write(d, p);
		}else{
			System.out.println("Variabili nulle");
		}
		
		

	}   
}
