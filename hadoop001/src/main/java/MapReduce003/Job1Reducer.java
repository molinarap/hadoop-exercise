package MapReduce003;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, IntWritable, Text, Text> {
    
    Text p = new Text();
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    	String[] k = key.toString().split("\t");
    	String[] coupleNumber = k[0].split(":");
    	double count = Double.parseDouble(k[1]);
    	
    	String couple = coupleNumber[0];
    	double n =Double.parseDouble(coupleNumber[1]);
    	
        int v = 0;
        for (IntWritable val : values) {
          v += val.get();
        }
        double l = v;
        double sum = (l/count)*100;
        double sum2 = (l/n)*100;
        p.set(sum+"%, "+sum2+"%");
        context.write(new Text(couple), p);
    }
    
}
