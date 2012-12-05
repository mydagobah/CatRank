package mapred.catrank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankCalculationReducer extends Reducer<Text, Text, Text, Text> {
	private static final float damping = 0.85F;
	
	protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
	
		boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;
        Iterator<Text> iter = values.iterator();
        
        // For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        while(iter.hasNext()){
            pageWithRank = iter.next().toString();
             
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                isExistingWikiPage = true;
                continue;
            }
 
            split = pageWithRank.split("\\s+");
            
            float pageRank;
            int countOutLinks;
            try {
            	pageRank = Float.parseFloat(split[1]);
                countOutLinks = Integer.parseInt(split[2]);
            } catch (NumberFormatException nfe) {
            	continue;
            }
                       
            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }
 
        if(!isExistingWikiPage) return;
        
        float newRank = damping * sumShareOtherPageRanks + (1-damping);
 
        context.write(key, new Text(newRank + links));
		
	}
}
