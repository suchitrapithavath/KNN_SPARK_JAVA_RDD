package edu.ucr.cs.cs226.spith001;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;



public class Knn {

	final static SparkConf sparkconf= new SparkConf().setAppName("knn").setMaster("local").set("spark.executor.memory","2g");
    static JavaSparkContext sc= new JavaSparkContext(sparkconf);
    static Integer k;
    static String inputfile;
    
	public static void main(String[] args) {
		k = Integer.parseInt(args[3]);// k value
		//String q=args[1]; //query point
		//String[] querypoint = q.split(",");//split by comma
		double x=Double.parseDouble(args[1]);//getting x coordinate
		double y =Double.parseDouble(args[2]);// getting y coordinate
		String inputfile = args[0];//input points file
	 	JavaRDD<String> data = sc.textFile(inputfile);//txt to RDD
	    JavaPairRDD<Double, String> knnmapped = data.mapToPair(new PairFunction<String, Double, String>() {
            @Override
            public Tuple2<Double, String> call(String line) throws Exception {
            	 String[] rTokens = line.split(","); 
    	         Double distance =((Math.sqrt(Math.pow((x-Double.parseDouble(rTokens[1])),2) + Math.pow((y-Double.parseDouble(rTokens[2])),2))));
    	                Tuple2<Double,String> V = new Tuple2<Double,String>(distance, line);
    	        return V;
            }

        })
        .sortByKey(false);// using javapairrdd calculating the Euclidean distance and sorting by key where key is distance 
	    List<Tuple2<Double,String>> knnlist= knnmapped.takeOrdered(k,new ListComparator());//getting top k values
	    JavaPairRDD<Double, String> rddKnnPoints = sc.parallelizePairs(knnlist);
	    rddKnnPoints.saveAsTextFile("sparkknnrdd2");	
	    sc.stop();
	}
	
	
	static class ListComparator implements Comparator<Tuple2<Double, String>>, Serializable {
		public int compare(Tuple2<Double, String> param1, Tuple2<Double, String> param2) {
			return param1._1.compareTo(param2._1);
		}
	}	
	

}
