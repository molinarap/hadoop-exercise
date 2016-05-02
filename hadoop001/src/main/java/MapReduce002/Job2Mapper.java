package MapReduce002;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text p = new Text();
	private Text dp = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] prodAll = line.split("\t");		
		String prodName = prodAll[0].toString();
		String prodDatePice = prodAll[1].toString();
		
		if (prodName != null && prodDatePice != null) {	
			p.set(prodName);
			dp.set(prodDatePice);
			context.write(p, dp);
		}else{
			System.out.println("Variabili nulle");
		}
		
		

	}   
}
