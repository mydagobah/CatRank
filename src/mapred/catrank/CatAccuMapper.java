package mapred.catrank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CatAccuMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
		
		int catTabIndex = value.find("\t");
 
        String cat = Text.decode(value.getBytes(), 0, catTabIndex);
        String count = Text.decode(value.getBytes(), catTabIndex+1, value.getLength()-(catTabIndex+1));

        context.write(new Text(cat), new Text(count));
		
	}
}
