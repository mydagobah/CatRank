package mapred.catrank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
		
		String[] pageAndRank = value.toString().split("\\s+");

        if (pageAndRank.length != 3) return;
        
		String page = pageAndRank[0];               
        String rank = pageAndRank[1];
        String links = pageAndRank[2];
		
        String[] allOtherPages = links.split(",");
        int totalLinks = allOtherPages.length;
        
        Text pageRankTotalLinks = new Text(page + "\t" + rank + "\t" + totalLinks);
        
        for (String otherPage : allOtherPages){           
            context.write(new Text(otherPage), pageRankTotalLinks);
        }
         
        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|"+links));
	}

}
