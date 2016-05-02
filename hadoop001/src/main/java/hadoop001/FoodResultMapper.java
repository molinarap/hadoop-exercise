package hadoop001;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FoodResultMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		int space_position = line.indexOf(" ");

		String date = line.substring(0, space_position);
		String product = line.substring(space_position);

		context.write(new Text(date), new Text(product));

	}   
}
