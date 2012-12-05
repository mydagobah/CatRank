package mapred.catrank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;

public class CatRankingReducer extends Reducer<Text, Text, Text, FloatWritable> {
	//private static final float damping = 0.85F;
	
	protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
	
		boolean isExistingWikiPage = false;
		String[] allCats;
		//String links = "";
		String rankOrCatLists;
		Iterator<Text> iter = values.iterator();
		float rank_f = 0;
		
		// For each otherPage: 
		// - check control characters
		// - calculate pageRank share <rank> / count(<links>)
		// - add the share to sumShareOtherPageRanks
		while(iter.hasNext()){
		    	rankOrCatLists = iter.next().toString();
		     
		    	if(rankOrCatLists.equals("!")) {
		        	isExistingWikiPage = true;
		        	continue;
		    	}
	
		    	if (isNumeric(rankOrCatLists)) {
				try {
		    			rank_f = Float.parseFloat(rankOrCatLists);
				} catch (NumberFormatException nfe) {
		    			continue;
		    		}
				continue;
		    	}
		
			if(!isExistingWikiPage) return;
		
			allCats = rankOrCatLists.split(",");
			int totalCats = allCats.length;

			FloatWritable rank = new FloatWritable(rank_f);

			for (String cat : allCats){
	    			context.write(new Text(cat), rank);
			}	
	
		}
	}

	private boolean isNumeric(String str) {
		return str.matches("\\d+(\\.\\d+)?");  //match a number 
	}
                
}


