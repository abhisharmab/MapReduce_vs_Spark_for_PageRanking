package spark;

import scala.Enumeration.Val;
import scala.Tuple2;
import spark.SparkDataClean.ConvertToWritableTypes;
import cern.colt.function.DoubleFunction;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.codehaus.jettison.json.*;
import org.apache.hadoop.io.compress.SnappyCodec;

import parquet.org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;


public final class AbhiPageRank implements Serializable{

	public AbhiPageRank() {};


	public static class ConvertToWritableTypes implements
	PairFunction<Tuple2<String, String>, Text, Text> {
		public Tuple2<Text, Text> call(Tuple2<String, String> record) {
			return new Tuple2(new Text(record._1), new Text(record._2));
		}
	}



	public static class SumofDangling implements Serializable
	{

		public Double total;

		public SumofDangling(Double total)
		{
			this.total = total;
		}
	}


	private static class Addition implements Function<String, Double> 
	{

		@Override
		public Double call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
	}

	public static void main(String[] args) throws Exception 
	{

		//if (args.length < 2) 
		//{
		//	System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
		//	System.exit(1);
		//}

		SparkConf sparkConf = new SparkConf().setAppName("AbhiPageRank");
		JavaSparkContext context = new JavaSparkContext(sparkConf);



		//Read files from S3 to create 
		JavaPairRDD<Text, Text> sequenceData = context.sequenceFile("s3n://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-00[0-2][0-9][0-9],s3n://aws-publicdatasets/common-crawl/parse-output/segment/1346876860819/metadata-00300", Text.class, Text.class);


		//Data Clean-Up Map
		JavaPairRDD<String, String> nativeFilteredData = sequenceData.mapToPair(new PairFunction<Tuple2<Text, Text>, String, String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<Text, Text> record) throws Exception 
			{
				try
				{
					String url = record._1.toString();
					String jsonData = record._2.toString();

					//Code to Extract URL out of JSON Begins
					int x = 0;
					Double defaultRank = new Double(1.0d);

					ObjectMapper mapper = new ObjectMapper();
					LinkedHashMap<String,Object> mainMap = mapper.readValue(jsonData.toString(), LinkedHashMap.class);

					if(mainMap != null)
					{
						LinkedHashMap<String, Object> linksMap =  (LinkedHashMap<String, Object>) mainMap.get("content");

						if(linksMap != null)
						{
							ArrayList<LinkedHashMap<String, String>> linkAsArray = (ArrayList<LinkedHashMap<String, String>>) linksMap.get("links");

							if(linkAsArray != null)
							{
								StringBuilder commaSeparated = new StringBuilder();

								for(LinkedHashMap<String, String> linkMap : linkAsArray)
								{
									if(linkMap.get("type").equals("a") && !linkMap.get("href").isEmpty() && !linkMap.get("href").contains(","))
									{
										if(x == 0)
										{
											commaSeparated.append(linkMap.get("href"));

											if(x == 0)
												x++;
										}
										else 
										{
											commaSeparated.append("," + linkMap.get("href"));
										}
									}
								}

								return new Tuple2<String, String>(url, String.valueOf(defaultRank) + "\t" + commaSeparated.toString());
							}
						}
					}
				}
				catch(Exception e)
				{
					//Do Nothing
				}
				//Code to Extract URL out of JSON Ends

				return new Tuple2<String, String>("random","1.0" +"\t" +"random");


			}

		});//.distinct(); //Expensive since data needs to be shuffled across the network


		//Data Clean-Up Reduce 

		nativeFilteredData = nativeFilteredData.groupByKey().mapValues(new Function<Iterable<String>, String>(){

			@Override
			public String call(Iterable<String> arg0) throws Exception {

				StringBuilder commaSeparated = new StringBuilder();
				HashSet<String> uniqueURLs = new HashSet<String>();
				Double defaultRank = new Double(1.0d);
				int i = 0;

				Iterator<String> vals = arg0.iterator();

				while(vals.hasNext())
				{
					String [] info = vals.next().split("\t");

					if(info.length >=2)
					{
						for(String url : info[1].split(","))
						{
							if(!uniqueURLs.contains(url))
								uniqueURLs.add(url);
						}

					}
				}
				
				for(String url : uniqueURLs)
				{
					if(i == 0)
					{
						commaSeparated.append(url);
					}
					else
					{
						commaSeparated.append("," + url);
					}

					if(i==0)
						i++;
				}

				return String.valueOf(defaultRank) + "\t" + commaSeparated.toString();
			}

		});
		
		
		/**
		nativeFilteredData = nativeFilteredData.reduceByKey(new Function2<String, String, String>(){

			@Override
			public String call(String value1, String value2) throws Exception 
			{
				StringBuilder commaSeparated = new StringBuilder();
				HashSet<String> uniqueURLs = new HashSet<String>();
				Double defaultRank = new Double(1.0d);
				int i = 0;

				if(value1 == null || value1.isEmpty())
				{
					return value2;
				}

				else if(value2 == null || value2.isEmpty())
				{
					return value1;
				}

				else
				{
					String [] data1 = value1.split("\t");
					String [] data2 = value2.split("\t");

					if(data1.length >=2 && !data1[1].isEmpty() && data1[1]!=null)
					{
						for(String url: data1[1].split(","))
						{
							if(!uniqueURLs.contains(url))
								uniqueURLs.add(url);

						}
					}

					if(data2.length >=2 && !data2[1].isEmpty() && data2[1]!=null)
					{
						for(String url: data2[1].split(","))
						{
							if(!uniqueURLs.contains(url))
								uniqueURLs.add(url);
						}
					}


					for(String url : uniqueURLs)
					{
						if(i == 0)
						{
							commaSeparated.append(url);
						}
						else
						{
							commaSeparated.append("," + url);
						}

						if(i==0)
							i++;
					}

					return String.valueOf(defaultRank) + "\t" + commaSeparated.toString();
				}

			}

		});*/

		//Page Rank Code Abhi

		for (int current = 0; current < 2; current++) //For Loop for Iterations
		{
			Timestamp ts = new Timestamp((new Date()).getTime());
			System.out.println("Current Time Stamp for Iteration " + current + ": " + ts);


			nativeFilteredData = nativeFilteredData.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){

				@Override
				public Iterable<Tuple2<String, String>> call(Tuple2<String, String> data) throws Exception 
				{
					Double pageRank = new Double(1.0d);
					List<Tuple2<String, String>> mappedData = new ArrayList<Tuple2<String, String>>();

					if(data != null)
					{
						String [] information = data._2().split("\t");
						String url = data._1();
						try
						{
							pageRank = Double.parseDouble(information[0]);
						}
						catch(NumberFormatException e)
						{
							//Do nothing
						}


						Double p = 0.0d;

						if(information.length < 2)
						{
							mappedData.add(new Tuple2<String, String>(url, String.valueOf(pageRank) + "\t" + "dangling"));
							return mappedData; 
						}


						if(information[1] != null && information[1] != "")
						{
							String [] numofLinks = information[1].split(",");

							//System.out.println("PageRank before: " + String.valueOf(pageRank));

							p = pageRank / numofLinks.length;

							//System.out.println("PageRank Divide: " + String.valueOf(p));
						}

						mappedData.add(new Tuple2<String, String>(url, String.valueOf(p) + "\t" + information[1]));


						if(information[1] != "" && information[1] != "dangling")
						{
							for(String link : information[1].split(","))
							{
								mappedData.add(new Tuple2<String, String>(link, String.valueOf(p)));

							}
						}

					}

					return mappedData;

				}

			});

			List<String> temp = nativeFilteredData.lookup("http://twitter.com/share");
			for(String t  : temp){
				System.out.println("Twitter     ============  "  + t);
			}




			//Get the #of Unique URLs
			Map<String, Object> uniqueURLs = nativeFilteredData.countByKey();
			final int uniqueURLCount = uniqueURLs.size();

			System.out.println("Total Number of UniqueURLs: " + String.valueOf(uniqueURLCount));


			/**
			//Addition done locally
			Function2<SumofDangling, Tuple2<String, String>, SumofDangling> addCount =
					new Function2<SumofDangling, Tuple2<String, String>, SumofDangling>() {

				@Override
				public SumofDangling call(SumofDangling arg0, Tuple2<String, String> data) throws Exception 
				{
					String [] information = data._2().split("\t");
					if(information.length >=2 && information[1].equalsIgnoreCase("dangling"))
					{
						try
						{
							arg0.total += Double.parseDouble(information[0]);
							return arg0;
						}
						catch(NumberFormatException e)
						{
							arg0.total += 0.0d;
							return arg0;
						}

					}
					else
					{
						arg0.total += 0.0d;
						return arg0;
					}
				}

			};


			//Addition done across the nodes
			Function2<SumofDangling, SumofDangling, SumofDangling> combine =
					new Function2<SumofDangling, SumofDangling, SumofDangling>() {

				@Override
				public SumofDangling call(SumofDangling arg0, SumofDangling arg1) throws Exception {
					arg0.total += arg1.total;
					return arg0;
				}

			};*/





			SumofDangling initial = new SumofDangling(0.0d);


			/**
			//Get the TOTAL SUM OF #DANGLING NODES across the Cluster 

			final String resultofSum = nativeFilteredData.values().reduce(new Function2<String, String, String>() {

				@Override
				public String call(String value1, String value2) throws Exception 
				{
					String [] info1 = value1.split("\t");
					String [] info2 = value2.split("\t");
					Double result = new Double(0.0d);

					if(info1.length >=2 && info1[1].equals("dangling"))
					{
						try
						{

							result += Double.parseDouble(info1[0]);

						}
						catch(NumberFormatException e)
						{
							//Do Nothing 
						}

					}

					if(info2.length >=2 && info2[1].equals("dangling"))
					{
						try
						{

							result += Double.parseDouble(info2[0]);

						}
						catch(NumberFormatException e)
						{
							//Do Nothing 
						}
					}

					return String.valueOf(result);
				}

			});

			Double sumofAllDangling = new Double(1.0d);

			try
			{
				sumofAllDangling = Double.parseDouble(resultofSum);

				if(sumofAllDangling == 0.0d)
				{
					sumofAllDangling = new Double(1.0d);
				}
			}
			catch(NumberFormatException e)
			{
				sumofAllDangling = new Double(1.0d);
			}

			final Double totalSumofDanglingNodes = sumofAllDangling;

			 */

			//Addition done locally
			Function2<SumofDangling, String, SumofDangling> addCount =
					new Function2<SumofDangling, String, SumofDangling>() {

				@Override
				public SumofDangling call(SumofDangling arg0, String data) throws Exception 
				{
					String [] information = data.split("\t");
					if(information.length >=2 && information[1].equalsIgnoreCase("dangling"))
					{
						try
						{
							arg0.total += Double.parseDouble(information[0]);
							return arg0;
						}
						catch(NumberFormatException e)
						{
							arg0.total += 0.0d;
							return arg0;
						}

					}
					else
					{
						arg0.total += 0.0d;
						return arg0;
					}
				}

			};


			//Addition done across the nodes
			Function2<SumofDangling, SumofDangling, SumofDangling> combine =
					new Function2<SumofDangling, SumofDangling, SumofDangling>() {

				@Override
				public SumofDangling call(SumofDangling arg0, SumofDangling arg1) throws Exception {
					arg0.total += arg1.total;
					return arg0;
				}

			};

			/**nativeFilteredData.aggregate(initial, addCount, combine).total; */
			final double totalSumofDanglingNodes = Math.round(nativeFilteredData.values().aggregate(initial, addCount, combine).total);


			System.out.println("Total Sum of DanglingNodes: " + String.valueOf(totalSumofDanglingNodes));

			/**
			List<Tuple2<String, String>> output = nativeFilteredData.collect();
			for(int i =0; i < 100 ; i++)
			{
				Tuple2<?,?> testTuple =  output.get(i);
				System.out.println(testTuple._1() + "" + testTuple._2());
			}*/

			//Page Rank Reduce Step
			nativeFilteredData = nativeFilteredData.reduceByKey(new Function2<String, String, String>(){

				@Override
				public String call(String value1, String value2) throws Exception 
				{
					Double s = new Double(0.0d);
					Double finalRank = new Double(0.0d);

					String data = new String();

					try
					{
						Double rank;
						if((rank = Double.parseDouble(value1)) != null)
						{
							s = s + rank;
							System.out.println("Inside value 1");
						}
					}
					catch(NumberFormatException e)
					{
						data = value1;
					}


					if(data.isEmpty())
					{
						try
						{
							Double rank;
							if((rank = Double.parseDouble(value2)) != null)
							{
								s = s + rank;
								System.out.println("Inside value 2");
							}
						}
						catch(NumberFormatException e)
						{
							data = value2;
						}
					}
					else
					{
						try
						{
							Double rank;
							if((rank = Double.parseDouble(value2)) != null)
							{
								s = s + rank;
								System.out.println("Inside value 2");
							}
						}
						catch(NumberFormatException e)
						{
							//Do Nothing
						}

					}


					String [] oldDataWithRank = data.split("\t");

					//System.out.println(String.valueOf(totalSumofDanglingNodes));
					//System.out.println(String.valueOf(uniqueURLCount));

					//Double test = totalSumofDanglingNodes/uniqueURLCount;

					//System.out.println("Value of Division: " + String.valueOf(test));

					finalRank = (1 - 0.85) + (0.85 * (s + (totalSumofDanglingNodes/uniqueURLCount)));

					//System.out.println("Value of Final Page Rank for Key: " + String.valueOf(finalRank));

					if(oldDataWithRank.length >= 2 && !data.isEmpty())
					{
						return String.valueOf(finalRank) + "\t" + oldDataWithRank[1];
					}
					else
					{
						return String.valueOf(finalRank) + "\t" + "dangling";
					}

				}

			});


			List<String> temp1 = nativeFilteredData.lookup("http://twitter.com/share");
			for(String t  : temp1){
				System.out.println("Twitter     ============  "  + t);
			}

			/**
			List<Tuple2<String, String>> output1 = nativeFilteredData.collect();
			for(int i =0; i < 100 ; i++)
			{
				Tuple2<?,?> testTuple =  output1.get(i);
				System.out.println(testTuple._1() + "" + testTuple._2());
			}*/

			//Cache-the RDD for future usage in the next iterations
			nativeFilteredData.cache();

			//Page Rank Code Abhi

			if(current == 1)
			{

				//JavaPairRDD<Text, Text> rdd = nativeFilteredData.mapToPair(new ConvertToWritableTypes());
				//rdd.saveAsHadoopFile("s3n://sparkabhi/blah2", Text.class, Text.class, org.apache.hadoop.mapred.SequenceFileOutputFormat.class, DefaultCodec.class); 

				//nativeFilteredData.saveAsHadoopFile("s3n://sparkabhi/testpagerank1", String.class, String.class, TextOutputFormat.class);
			}
		}

		//nativeFilteredData.saveAsHadoopFile("s3n://sparkabhi/cleanestdata", String.class, String.class, TextOutputFormat.class);
		context.stop();
	}

}



