package hadoop001.hadoop001;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FoodMaxReducer extends Reducer<Text, IntWritable, Text, Text> {
        
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String[] new_item = key.toString().split(" ");
        String new_value = new_item[1] + " " + values ;
        String new_date = new_item[0];
        context.write(new Text(new_date), new Text(new_value));
    }
    
}
