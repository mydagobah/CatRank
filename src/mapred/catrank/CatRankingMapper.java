package mapred.catrank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CatRankingMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
		
	int pageTabIndex = value.find("\t");	
	String page = Text.decode(value.getBytes(), 0, pageTabIndex);

	String rankOrCats = Text.decode(value.getBytes(),pageTabIndex+1, value.getLength()-(pageTabIndex+1));

	if (isNumeric(rankOrCats))
		context.write(new Text(page), new Text("!"));	
		context.write(new Text(page), new Text(rankOrCats));	
	}

	private boolean isNumeric(String str) {
		return str.matches("\\d+(\\.\\d+)?");  //match a number 
	}
}


