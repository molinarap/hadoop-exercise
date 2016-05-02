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

	private Map<String, Integer> countProd = new HashMap<String,Integer>();

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValues( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
		{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				return (o2.getValue()).compareTo(o1.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text val : values) {
			String v = val.toString();
			String[] all = v.split(" ");
			int n = Integer.parseInt(all[1]);
			String p = all[0].toString();
			countProd.put(p, n);
		}

		Map<String, Integer> sortProd = sortByValues(countProd);
		
		int count = 0;
		String result = "";
		for(String s: sortProd.keySet()){
			if(count++ ==5){
				break;
			}
			
			result += s + " " + sortProd.get(s) + ", ";
		}
		
		result = result.substring(0, result.length()-2);
		
		context.write(key, new Text(result));
		
	}

}
