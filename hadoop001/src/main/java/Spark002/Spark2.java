package Spark002;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Spark2 {

	private static String pathToFile;
	private static final String MISSING = "";

	public Spark2(String file){
		this.pathToFile = file;
	}

	/**
	 *  Load the data from the text file and return an RDD of foods
	 */
	public JavaRDD<String> loadData() {

		SparkConf conf = new SparkConf()
				.setAppName("Spark2")
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

	public JavaPairRDD<String, String> foodPrice() {
		JavaPairRDD<String, Integer> priceFoods = foodCount();

		JavaPairRDD<String,String> mapFoods = priceFoods.mapToPair(p -> {

			String line = p.toString().substring(1, p.toString().length()-1);

			Tuple2<String, String> result = null;

			// 2015-10 formaggio/8,30
			String[] prodDateNameTimeFalse = line.split(" ");

			// 2015-10
			String prodDate = prodDateNameTimeFalse[0].toString();
			// formaggio/8,30
			String prodNameTimeFalse = prodDateNameTimeFalse[1].toString();

			String[] prodNameTime = prodNameTimeFalse.split("/");
			// formaggio
			String prodName = prodNameTime[0];
			// 8,30
			String prodTime = prodNameTime[1];
			
			// 2015-10 formaggio/8,30
			
			String[] prodTimePriceFalse = prodTime.split(",");

			int prodTimePrice = Integer.parseInt(prodTimePriceFalse[0]) * Integer.parseInt(prodTimePriceFalse[1]);

			String priceInMonth = prodDate + ":" + Integer.toString(prodTimePrice);

			if (prodName != null && priceInMonth != null) {	
				result = new Tuple2<String, String>(prodName, priceInMonth);
			}else{
				System.out.println("Variabili nulle");
			}

			return result;

		});

		JavaPairRDD<String, String> finalResult = mapFoods.reduceByKey((a, b) -> a + " " + b);

		return finalResult;

	}


	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: Food <filetxt>");
			System.exit(1);
		}

		Spark2 spk = new Spark2(args[0]);

		for(Tuple2<String, String> item:spk.foodPrice().collect()){
			System.out.println(item._1 + " "+ item._2 + ", ");
		}

	}
}








