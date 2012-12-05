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
		
		// Optimization
		//conf.setInt("mapred.map.tasks", 16);
		//conf.setInt("mapred.block.size", 268435456); // blk size = 256 M
		//conf.setInt("mapred.max.split.size", 536870912);
		conf.setInt("mapred.min.split.size", 134217728); // min 128M
		conf.setBoolean("mapred.compress.map.output", true); // compression
		
		
		// select the <page> tag 
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		//conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
				
		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Parse page info from xml files");
		job._setInputFormatClass(XmlInputFormat.class);
		job.setClasses(PageParsingMapper.class, PageParsingReducer.class, PageParsingCombiner.class);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();	
	}
	
	/*
	 * Mapreduce Job 2 - calculate page ranks iteratively
	 */
	private static void calculateRank(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
			
		Configuration conf = new Configuration();
		conf.setInt("mapred.min.split.size", 134217728);
		//conf.setInt("mapred.inmem.merge.threshold", 0);
		//conf.setFloat("mapred.job.reduce.input.buffer.percent", 1.0f);
		
		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Calculate page ranks");		
		job._setInputFormatClass(TextInputFormat.class);
		job.setClasses(RankCalculationMapper.class, RankCalculationReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();	
	}
	
	/*
	 * Mapreduce Job 3 - output the page ranks in order
	 */
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
