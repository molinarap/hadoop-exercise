package MapReduce003;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private final static IntWritable SUM = new IntWritable();
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        SUM.set(sum);
        context.write(new Text(key.toString()), SUM);
    }
    
}
