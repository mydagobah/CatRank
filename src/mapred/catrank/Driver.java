package mapred.catrank;

import java.io.IOException;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import mapred.util.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		//String tmpdir = parser.get("tmpdir");

		// job1
		parseXml(input, output);
		
	}

	private static void parseXml(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		// setup deliminator of content to parse
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		//conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		
		
		// setup job
		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Parse page info from xml files");
		//FileInputFormat.setInputPaths(job, input);
		job._setInputFormatClass(XmlInputFormat.class);
		job.setClasses(WikiPageLinksMapper.class, WikiPageLinksReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(1);
		job.run();
		
		
		/*
		Job job = new Job(conf, "Parse page info from xml files");
		//job.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job, input);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(WikiPageLinksMapper.class);
		
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setOutputFormatClass(TextOutputFormat.class);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(WikiPageLinksReducer.class);
		//job.setNumReduceTasks(0);
		
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) dfs.delete(outPath, true);
		
		job.waitForCompletion(true);
		*/
	}
}
