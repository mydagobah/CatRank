package mapred.catrank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CatParsingReducer extends Reducer<Text, Text, Text, Text> {
    
    protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
        
    	String cats = "";
    	Iterator<Text> iter = values.iterator();  	
    	
        if (iter.hasNext()) cats += iter.next().toString();
        
        while(iter.hasNext()){
            cats += ",";      
            cats += iter.next().toString();
        }
        
        // output: pageA   Cat1,Cat2,CatN
        context.write(key, new Text(cats));
    }
}
