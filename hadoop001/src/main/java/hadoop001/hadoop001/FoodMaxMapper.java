package hadoop001.hadoop001;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FoodMaxMapper extends Mapper<Text, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private static final String MISSING = "";

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String date = line.substring(0, 6);
		String stringProd = null;

		if (line.charAt(8) == ',') {
			stringProd = line.substring(9);
		}

		if (line.charAt(9) == ',') {
			stringProd = line.substring(10);
		}

		if (line.charAt(10) == ',') {
			stringProd = line.substring(11);
		}

		if ( stringProd != MISSING ) {			
			String[] arrayProd = stringProd.split(",");
			for (int x=0; x<arrayProd.length; x++){
				String all = date + " " + arrayProd[x];
				Text t = new Text(all);
				context.write(t, ONE);
			}

		}
	}   
}
