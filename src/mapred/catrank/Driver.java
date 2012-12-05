package mapred.catrank;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import mapred.util.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

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
		orderRanks(tmpdir + "/run" + (runs), tmpdir + "/PRresult");

		// job4
		catParse(input, tmpdir + "/catParse");

		// job5
		catRanks(tmpdir + "/catParse", tmpdir + "/PRresult",  tmpdir + "/catCalc");

		// job6
		catAccumulate(tmpdir + "/catCalc", output);

	}

	/*
	 * Mapreduce Job 1 - parse page relationships from raw XML files
	 */
	private static void parseXml(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		// setup configuration
		Configuration conf = new Configuration();
		
		// Optimization
		conf.setInt("mapred.min.split.size", 134217728);     // min 128M
		conf.setBoolean("mapred.compress.map.output", true); // compression
				
		// select the <page> tag 
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
				
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
		job.setMapOutputClasses(Text.class, FloatWritable.class);
		//job.setReduceJobs(1);
		job.run();	
	}

	/*
	 * Mapreduce Job 4 - Parse category info from xml file
	 */
	private static void catParse(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Parse category info from xml files");
		
		job._setInputFormatClass(XmlInputFormat.class);
		job.setClasses(CatParsingMapper.class, CatParsingReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();	
	}

	/*
	 * Mapreduce Job 5 - Order category ranks
	 */
	private static void catRanks(String input, String rankfile, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		JobConf jobconf = new JobConf(conf);
		FileInputFormat.addInputPath(jobconf, new Path(rankfile));

		// setup job		
		Optimizedjob job = new Optimizedjob(jobconf, input, output, "Order category ranks");
		
		job._setInputFormatClass(TextInputFormat.class);
		job.setClasses(CatRankingMapper.class, CatRankingReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();	
	}

	/*
	 * Mapreduce Job 6 - accumulate category rank
	 */
	private static void catAccumulate(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		// setup job		
		Optimizedjob job = new Optimizedjob(conf, input, output, "Calculate category rank");
		
		job._setInputFormatClass(TextInputFormat.class);
		job.setClasses(CatAccuMapper.class, CatAccuReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(1);
		job.run();	
	}

	
}
