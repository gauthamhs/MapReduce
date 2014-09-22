import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.DistributedCache;

public class WCDistCache {
	//standard Map Class from the word count example
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public Path[] localFiles;
		private Set<String> patternsToCheck = new HashSet<String>();

		//This method is used to create the local copy of a set of words from the patterns file
		public void configure(JobConf job) {
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
				BufferedReader fis = new BufferedReader(new FileReader(
						localFiles[0].toString()));
				
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					StringTokenizer tokenizer = new StringTokenizer(pattern);
					while (tokenizer.hasMoreTokens()) {
						String thisString = tokenizer.nextToken();
						//local map created
						patternsToCheck.add(thisString);
					}
				}
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//standard Map function from the word count example
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String thisString = tokenizer.nextToken();
				// increment the count only if the word is in the map
				if(patternsToCheck.contains(thisString)){
					word.set(thisString);
					output.collect(word, one);
				}
			}
		}
	}
	
	//standard reduce function
	public static class Reduce extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WCDistCache.class);
		conf.setJobName("wordcount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//For AWS
		DistributedCache.addCacheFile(new URI("s3://rahul-ufl-bucket1/input-pattern/pattern.txt"),conf);
		//For FutureGrid
		//DistributedCache.addCacheFile(new URI("word-patterns.txt"),conf);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
