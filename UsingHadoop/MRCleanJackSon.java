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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
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
public class MRCleanJackSon{


	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{

		JobConf jobConf = new JobConf(MRCleanUp.class);

		jobConf.setJobName("MRCleanUp");

		jobConf.setMapOutputKeyClass(Text.class);   
		jobConf.setMapOutputValueClass(Text.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		jobConf.setMapperClass(CleanUpMapper.class);
		jobConf.setReducerClass(CleanUpReducer.class);

		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);


		//jobConf.setBoolean("mapred.output.compress",true);
		//jobConf.set("mapred.output.compression.type", "BLOCK");     
		//jobConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
		

		TextOutputFormat.setCompressOutput(jobConf, true);
		
		jobConf.setNumReduceTasks(46);

		FileSystem fs = FileSystem.get(URI.create(args[0]), jobConf);

		FileStatus[] inputFiles = fs.listStatus(new Path(args[0]));

		if(inputFiles != null)
		{
			for(FileStatus file : inputFiles)
			{
				if(file.toString().contains("metadata-00"))
				{
				    Integer index =file.toString().indexOf("metadata-00");
				    String number = file.toString().substring(index+11, index+11+3);

					Integer fileNum = Integer.parseInt(number);

					if( 0 <= fileNum  && fileNum <= 300)
					{
						FileInputFormat.addInputPath(jobConf, file.getPath());
					}
				}
			}
		}


		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));  

		JobClient.runJob(jobConf);

	}


	public static class CleanUpMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
	{
		@Override
		public void map(Text url, Text pointerLinks, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			int x = 0;
			Double defaultRank = new Double(1.0d);

			long numRecords = 0;

			//
			ObjectMapper mapper = new ObjectMapper();
			LinkedHashMap<String,Object> mainMap = mapper.readValue(pointerLinks.toString(), LinkedHashMap.class);

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
							if(linkMap.get("type").equals("a") && !linkMap.get("href").isEmpty())
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

						output.collect(url, new Text(String.valueOf(defaultRank) + "\t" + commaSeparated.toString()));
					}
				}
			}
			//


			if((++numRecords % 100) == 0)
				reporter.setStatus("Finished processing: " + numRecords);
		}

	}


	public static class CleanUpReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		Double defaultRank = new Double(1.0d);

		@Override
		public void reduce(Text url, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{			
			StringBuilder commaSeparated = new StringBuilder();
			int i = 0;

			while(values.hasNext())
			{
				String [] links = values.next().toString().split("\t");

				if(i==0)
				{
					if(links.length >= 2)
						commaSeparated.append(links[1]);
				}
				else
				{
					if(links.length >=2)
						commaSeparated.append("," + links[1]);
				}

				if(i==0)
					i++;	
			}

			output.collect(url, new Text(String.valueOf(defaultRank) + "\t" + commaSeparated.toString()));
		}

	}

}




