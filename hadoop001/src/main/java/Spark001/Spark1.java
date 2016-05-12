package Spark001;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Spark1 {

	private static String pathToFile;
	private static final String MISSING = "";

	public Spark1(String file){
		this.pathToFile = file;
	}

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

	/**
	 *  Load the data from the text file and return an RDD of foods
	 */
	public JavaRDD<String> loadData() {

		SparkConf conf = new SparkConf()
				.setAppName("Spark1")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> foods = sc.textFile(pathToFile).flatMap(line -> Arrays.asList(line.split("/n")));

		JavaRDD<String> dateFoods = foods.flatMap(food -> {

			String date = null;
			String stringProd = null;
			ArrayList<String> allFood = new ArrayList<String>();
			if (food.charAt(8) == ',') {
				date = food.substring(0, 6);
				stringProd = food.substring(9);
			}

			if (food.charAt(9) == ',') {
				if (food.charAt(7) == '-') {
					date = food.substring(0, 7);
				}
				if (food.charAt(6) == '-') {
					date = food.substring(0, 6);
				}
				stringProd = food.substring(10);
			}

			if (food.charAt(10) == ',') {
				date = food.substring(0, 7);
				stringProd = food.substring(11);
			}

			if ( stringProd != MISSING ) {			
				String[] arrayProd = stringProd.split(",");
				for (int x=0; x<arrayProd.length; x++){
					String all = date + " " + arrayProd[x];
					allFood.add(all);
				}
			}
			return allFood;
		});

		return dateFoods;

	}

	public JavaPairRDD<String, Integer> foodCount() {
		JavaRDD<String> dateFoods = loadData();

		JavaPairRDD<String, Integer> mapFoods = dateFoods.mapToPair(f -> new Tuple2<String, Integer>(f, 1));

		JavaPairRDD<String, Integer> result = mapFoods.reduceByKey((a, b) -> a + b);

		return result;

	}

	public JavaPairRDD<String, String> foodOrder() {
		JavaPairRDD<String, Integer> listFoods = foodCount();

		JavaPairRDD<String,String> mapFoods = listFoods.mapToPair(p -> {

			String line = p.toString().substring(1, p.toString().length()-1);

			System.out.println("LINE" + line);

			Tuple2<String, String> result = null;

			// 2015-5 mozzarella 37,
			String[] prodDateNameTimeFalse = line.split(" ");

			// 2015-10
			String prodDate = prodDateNameTimeFalse[0];
			// formaggio,30
			String prodNameTimeFalse = prodDateNameTimeFalse[1];
			
			String[] prodNameTime = prodNameTimeFalse.split(",");

			// 30

			String priceInMonth = prodNameTime[0] + " " + prodNameTime[1];

			if (prodDate != null && priceInMonth != null) {	
				result = new Tuple2<String, String>(prodDate, priceInMonth);
			}else{
				System.out.println("Variabili nulle");
			}

			return result;

		});
		
		JavaPairRDD<String, String> result = mapFoods.reduceByKey((a, b) -> a + ", " + b);

		return result;
	};


	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: Food <filetxt>");
			System.exit(1);
		}

		Spark1 spk = new Spark1(args[0]);

		for(Tuple2<String, String> item:spk.foodOrder().collect()){
			System.out.println(item._1 + " "+ item._2);
		}

	}
}







