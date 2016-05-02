package MapReduce002;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, IntWritable, Text, Text> {
        
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
        String[] new_item = key.toString().split(" ");
        int v = 0;
        for (IntWritable val : values) {
            v = val.get();
        }
       
        //new_item[0] = 2015-1
        //new_item[1] = acqua/3 
        
        String new_date = new_item[0];

        String new_value = new_item[1];
        
        String[] namePrice = new_value.split("/");
        String nameProd = namePrice[0];
		String priceProd = namePrice[1];
		int totalPriceProd = Integer.parseInt(priceProd) * v;
		
        String datePrice = new_date + ":" + totalPriceProd;
        context.write(new Text(nameProd), new Text(datePrice));
    }
    
}
