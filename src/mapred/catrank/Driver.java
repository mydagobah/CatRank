package mapred.catrank;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		//String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		// job1
		parseXml(input, tmpdir);
		
	}

	private static void parseXml(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for hashtag #Job");
		
		job.setClasses(WikiPageLinksMapper.class, WikiPageLinksReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(1);

		job.run();

		/*
		JobConf conf = new JobConf(WikiPageRanking.class);
        
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        
        // Input / Mapper
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(WikiPageLinksMapper.class);
        
        // Output / Reducer
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(WikiLinksReducer.class);
        
        JobClient.runJob(conf);
	*/
	}
}
