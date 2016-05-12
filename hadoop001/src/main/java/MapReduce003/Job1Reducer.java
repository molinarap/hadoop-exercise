package MapReduce003;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, IntWritable, Text, Text> {
    
    Text p = new Text();
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    	String[] k = key.toString().split("\t");
    	String couple = k[0];
    	double count = Double.parseDouble(k[1]);
    	
        int v = 0;
        for (IntWritable val : values) {
          v += val.get();
        }
        double l = v;
        double sum = (l/count)*100;
        p.set(sum+"%");
        context.write(new Text(couple), p);
    }
    
}
