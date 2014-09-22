import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountPair {
	//standard MAP class taken from the word count example
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		//standard Map function from the word count example
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			// this always saves the first word of the pair of words
			String word1 = "";
			while (tokenizer.hasMoreTokens()) {
				String thisString = tokenizer.nextToken();
				// check if the string with '' or ' as mentioned on asana
				if (thisString.startsWith("''")) {
					thisString = thisString.substring(2);
				}
				else if (thisString.startsWith("'")) {
					thisString = thisString.substring(1);
				}
				
				// this check is required for the begining of the line, so as to ignore the first word alone
				if(word1 == ""){
					word1 = thisString;
					continue;
				}
				//creating the pair
				String wordPair = word1 + " " + thisString;
				word1 = thisString;
				word.set(wordPair);
				output.collect(word, one);
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

	// this takes the input and output folders paths as program arguments
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCountPair.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
