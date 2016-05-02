package MapReduce002;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, String> countProd = new HashMap<String,String>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text val : values) {
			String v = val.toString();
			String[] all = v.split(":");
			String p = all[0].toString();
			String dp = all[1].toString();
			countProd.put(p, dp);
		}

		String result = "";
		for(String s: countProd.keySet()){
			result += s + ":" + countProd.get(s) + " ";
		}

		context.write(key, new Text(result));

	}

}
