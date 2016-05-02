package hadoop001;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FoodResultReducer extends Reducer<Text, Text, Text, Text> {
        
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    	String v = null;
        for (Text val : values) {
            v = val.toString();
        }
        
        context.write(key, new Text(v));
    }
    
}
