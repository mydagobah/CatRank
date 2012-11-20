package mapred.catrank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class WikiPageLinksReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String pagerank = "1.0\t";

        boolean first = true;
        while(values.hasNext()){
            if(!first) pagerank += ",";
            
            pagerank += values.next().toString();
            first = false;
        }
        
        output.collect(key, new Text(pagerank));
    }
}
