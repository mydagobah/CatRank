package mapred.catrank;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankOrderingMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {

		String[] pageAndRank = value.toString().split("\\s+");
        
        context.write(new FloatWritable(Float.parseFloat(pageAndRank[1])), 
        		      new Text(pageAndRank[0]));		
	}
}
