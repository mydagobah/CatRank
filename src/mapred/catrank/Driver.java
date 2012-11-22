package mapred.catrank;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import mapred.util.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		//String tmpdir = parser.get("tmpdir");

		// job1
		parseXml(input, output);
	
		// job2
	}

	private static void parseXml(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		// setup configuration
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
	}
}
