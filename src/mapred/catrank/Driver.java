package mapred.catrank;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import mapred.util.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Driver {

	/**
	 * This is the main class Hadoop will start from.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		// job1
		parseXml(input, tmpdir + "/run0");
	
		// job2 
		int runs = 0;
		for(; runs < 5; runs++) {
			calculateRank(tmpdir + "/run" + runs, tmpdir + "/run" + (runs + 1));
		}
		
		// job3
		orderRanks(tmpdir + "/run" + (runs), output);
	}

	/*
	 * Mapreduce Job 1 - parse page relationships from raw XML files
	 */
	private static void parseXml(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		// setup configuration
		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		//conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
				
		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Parse page info from xml files");
		//FileInputFormat.setInputPaths(job, input);
		job._setInputFormatClass(XmlInputFormat.class);
		job.setClasses(WikiPageLinksMapper.class, WikiPageLinksReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();	
	}
	
	private static void calculateRank(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
			
		Configuration conf = new Configuration();
		
		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Calculate page ranks");
		
		job._setInputFormatClass(TextInputFormat.class);
		job.setClasses(RankCalculationMapper.class, RankCalculationReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		//job.setReduceJobs(1);
		job.run();	
	}
	
	private static void orderRanks(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Order page ranks");
		
		job._setInputFormatClass(TextInputFormat.class);
		job.setClasses(RankOrderingMapper.class, null, null);
		job.setMapOutputClasses(FloatWritable.class, Text.class);
		job.setReduceJobs(1);
		job.run();	
	}
	
}
