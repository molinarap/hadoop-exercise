package MapReduce003;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1Main {

    public static void main(String[] args) throws Exception {
    	
    	Job job = new Job(new Configuration(), "FoodJob1Main");

		job.setJarByClass(Job1Main.class);
		
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		
    }
}
