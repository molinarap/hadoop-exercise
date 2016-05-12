package Spark003;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Spark3 {

	private static String pathToFile;
	private static final String MISSING = "";

	public Spark3(String file){
		this.pathToFile = file;
	}
	
	public static int countLines() throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(pathToFile));
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
			FileReader fr = new FileReader(pathToFile);
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

			String line = food.toString();
			String[] arrayProd = line.split(",");

			ArrayList<String> l = new ArrayList<String>();

			HashMap<String, Integer> mapElem = countElemLines();
			int c = countLines();


			for(int i=1; i<arrayProd.length; i++){
				for(int j=1; j<arrayProd.length; j++){
					if(i!=j){
						if(mapElem.containsKey(arrayProd[i])){
							int q = mapElem.get(arrayProd[i]);
							String couple = arrayProd[i]+","+arrayProd[j]+":"+q+ '\t' + c;
							l.add(couple);
						}
					}
				}
			}
			
			return l;
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
			
			Tuple2<String, String> result = null;
			
			// acqua,birra:345	1000 135
			String line = p.toString().substring(1, p.toString().length()-1);

			String[] k = line.toString().split("\t");
	    	
			// 1000,135
			String[] countTemp = k[1].split(",");
			
			// 1000 135
	    	double count = Double.parseDouble(countTemp[0]);
	    	
			// 1000 135
	    	double l = Double.parseDouble(countTemp[1]);
			
	    	// acqua,birra:345
	    	String[] coupleNumber = k[0].split(":");
	    	String couple = coupleNumber[0];
	    	double n =Double.parseDouble(coupleNumber[1]);
	    	
	        double sum = (l/count)*100;
	        double sum2 = (l/n)*100;
	        String totSum = sum+"%, "+sum2+"%";
			result = new Tuple2<String, String>(couple, totSum);

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

		Spark3 spk = new Spark3(args[0]);

		for(Tuple2<String, String> item:spk.foodPrice().collect()){
			System.out.println(item._1 + " "+ item._2 + ", ");
		}

	}
}








