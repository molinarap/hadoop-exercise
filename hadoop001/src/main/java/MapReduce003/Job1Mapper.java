package MapReduce003;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

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

	public static HashMap<String, Integer> countElemLines() throws IOException {
		HashMap<String, Integer> m = new HashMap<String, Integer>();

		try {
			FileReader fr = new FileReader(path);
			BufferedReader br = new BufferedReader(fr);

			String line = br.readLine();
			while (line != null) {
				String[] parts = line.split(",");
				for (String w : parts) {
					if(w.substring(0, 4)!="2015"){
						if(!m.containsKey(w)){
							m.put(w, 1);
						}else{
							m.put(w, m.get(w) + 1);
						}
					}
				}
				line = br.readLine();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return m;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] arrayProd = line.split(",");

		ArrayList<String> l = new ArrayList<String>();

		HashMap<String, Integer> mapElem = countElemLines();

		for(int i=1; i<arrayProd.length; i++){
			for(int j=1; j<arrayProd.length; j++){
				if(i!=j){
					if(mapElem.containsKey(arrayProd[i])){
						int q = mapElem.get(arrayProd[i]);
						String couple = arrayProd[i]+","+arrayProd[j]+":"+q;
						l.add(couple);
					}
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
