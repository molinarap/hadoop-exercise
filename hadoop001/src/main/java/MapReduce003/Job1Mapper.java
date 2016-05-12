package MapReduce003;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private static final String path = "food.txt";
	private Text t = new Text();

	public static int countLines() throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(path));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	} 
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] arrayProd = line.split(",");
		
		ArrayList<String> l = new ArrayList<String>();
		
		for(int i=1; i<arrayProd.length; i++){
			for(int j=1; j<arrayProd.length; j++){
				if(i!=j){
					String couple = arrayProd[i]+","+arrayProd[j];
					l.add(couple);
				}
			}
		}
		
		int c = countLines();

			for (int x=0; x<l.size(); x++){
				String coupleLines = l.get(x) + '\t' + c;
				t.set(coupleLines);
				context.write(t, ONE);
			}
	}   
}
