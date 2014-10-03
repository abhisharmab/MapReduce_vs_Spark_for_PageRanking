/**
 * 
 */
package dataclean;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * @author abhisheksharma
 *
 */
public class PageRank {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		int iterationCount = 0; 

		JobConf jobConf = new JobConf(PageRank.class);

		jobConf.setJobName("PageRankIterative");

		jobConf.setMapOutputKeyClass(Text.class);   
		jobConf.setMapOutputValueClass(Text.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		jobConf.setMapperClass(PageRankMapper.class);
		jobConf.setReducerClass(PageRankReducer.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
	
		jobConf.setNumReduceTasks(46);
		
		TextOutputFormat.setCompressOutput(jobConf, true);

		String tempFolder = "/usr/temp/";
		
		FileSystem fs = FileSystem.get(jobConf);


		while( iterationCount < 10)
		{	

				
			String input, output;

			if (iterationCount == 0)
				input = args[0];
			else
				input = tempFolder + iterationCount;

			output = tempFolder + (iterationCount + 1); 

			FileInputFormat.setInputPaths(jobConf, new Path(input)); // setting the input files for the job
			
			if(iterationCount == 9)
				FileOutputFormat.setOutputPath(jobConf, new Path(args[1] + (iterationCount + 1)));
			
			else
				FileOutputFormat.setOutputPath(jobConf, new Path(output)); // setting the output files for the job

			JobClient.runJob(jobConf);
			
			if(fs.exists(new Path(tempFolder + (iterationCount - 1))))
				fs.delete(new Path(tempFolder + (iterationCount - 1)), true);
			
			iterationCount++;

		}



	}

	public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{

		@Override
		public void map(LongWritable number, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			Double pageRank = new Double(1.0d);

			if(value != null && !value.toString().isEmpty())
			{
				String [] data = value.toString().split("\t");
				String url = data[0].toString();
				pageRank = Double.parseDouble(data[1]);


				Double p = 0.0d;

				if(data.length < 3)
				{
					output.collect(new Text(url) , new Text(String.valueOf(pageRank) + "\t" + ""));
					return; 
				}


				if(data[2] != null && !data[2].isEmpty() && data[2] != "dangling")
				{
					String [] numofLinks = data[2].split(",");

					p = pageRank / numofLinks.length;
				}

				output.collect(new Text(url) , new Text(String.valueOf(pageRank) + "\t" + data[2]));

				if(data[2] != "" && data[2] != "dangling")
				{
					for(String link : data[2].split(","))
					{
						output.collect(new Text(link), new Text(String.valueOf(p)));
					}
				}

			}

		}

	}


	public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{

		@Override
		public void reduce(Text url, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			Double s = new Double(0.0d);
			Double finalRank = new Double(0.0d);

			String data = new String();

			while(values.hasNext())
			{
				String val = values.next().toString();
				
				try
				{
					Double rank;
					if((rank = Double.valueOf(val)) != null)
					{
						s = s + rank;
					}
				}
				catch(NumberFormatException e)
				{
					data = val;
				}
			}

			String [] oldDataWithRank = data.split("\t");

			finalRank = (1 - 0.85) + (0.85 * s);

			if(oldDataWithRank.length >= 2 && data != null)
			{
				output.collect(url, new Text(String.valueOf(finalRank) + "\t" + oldDataWithRank[1]));
			}
			else
			{
				output.collect(url, new Text(String.valueOf(finalRank) + "\t" + "dangling"));
			}
		}

	}


}
