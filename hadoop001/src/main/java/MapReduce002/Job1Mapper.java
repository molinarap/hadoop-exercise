package MapReduce002;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private static final String MISSING = "";
	private Text t = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String date = null;
		String stringProd = null;

		if (line.charAt(8) == ',') {
			date = line.substring(0, 6);
			stringProd = line.substring(9);
		}

		if (line.charAt(9) == ',') {
			if (line.charAt(7) == '-') {
				date = line.substring(0, 7);

			}
			if (line.charAt(6) == '-') {
				date = line.substring(0, 6);
			}
			stringProd = line.substring(10);
		}

		if (line.charAt(10) == ',') {
			date = line.substring(0, 7);
			stringProd = line.substring(11);
		}

		if ( stringProd != MISSING ) {			
			String[] arrayProd = stringProd.split(",");
			for (int x=0; x<arrayProd.length; x++){
				String all = date + " " + arrayProd[x];
				t.set(all);
				context.write(t, ONE);
			}

		}
	}   
}
