package mapred.catrank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;


public class CatAccuReducer extends Reducer<Text, Text, FloatWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float count = 0;
		String valueString = "";

		Iterator<Text> iter = values.iterator();

		while(iter.hasNext()){
            valueString = iter.next().toString();
             
            try {
			    count += Float.parseFloat(valueString);
			} catch (NumberFormatException nfe) {
				continue;
			}
		}
		
		context.write(new FloatWritable(count), key);
	}
}
