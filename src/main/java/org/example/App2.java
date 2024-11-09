package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        // Initialize Spark configuration and set the application name and master
        SparkConf conf = new SparkConf().setAppName("Total Sales By City and Year").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Year to filter the sales data
        String targetYear = "2022";

        // Read lines from the text file "ventes.txt" into an RDD
        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        // Split each line into an array of strings (date, ville, produit, prix)
        JavaRDD<String[]> rddExtract = rddLines.map(line -> line.split(" "));

        // Filter the sales data by the target year
        JavaRDD<String[]> rddFilteredByYear = rddExtract.filter(item -> item[0].startsWith(targetYear));

        // Extract the city (ville) and sales amount (prix) for each sale
        JavaPairRDD<String, Double> rddCitySales = rddFilteredByYear.mapToPair(
                item -> new Tuple2<>(item[1], Double.parseDouble(item[3])));

        // Reduce by key to sum the sales for each city
        JavaPairRDD<String, Double> rddTotalSalesByCity = rddCitySales.reduceByKey((a, b) -> a + b);

        // Print each city and its total sales
        rddTotalSalesByCity.foreach(e -> System.out.println("City: " + e._1 + ", Total Sales: " + e._2));



    }
}