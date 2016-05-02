package hadoop001;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
        
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    	String all = "";
        for (Text val : values) {
        	all = all + val.toString() + " ";
        }
        
        all = all.substring(0, all.length()-2);
        context.write(key, new Text(all));
    }
    
}
