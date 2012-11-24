package mapred.catrank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageParsingReducer extends Reducer<Text, Text, Text, Text> {
    
    protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
        
    	String pagelinks = "1.0\t";
    	Iterator<Text> iter = values.iterator();  	
    	
        if (iter.hasNext()) pagelinks += iter.next().toString();
        
        while(iter.hasNext()){
            pagelinks += ",";      
            pagelinks += iter.next().toString();
        }
        
        // output: pageA   1.0   pageB,pageC,pageN
        context.write(key, new Text(pagelinks));
    }
}
