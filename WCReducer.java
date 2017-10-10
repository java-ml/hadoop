package sort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MR:Reduce,reduce
 */
public class WCReducer extends Reducer<CompKey, NullWritable, Text,IntWritable> {

	protected void reduce(CompKey key, Iterable<NullWritable> values,
			Reducer<CompKey, NullWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	
		    Text year = key.year;
			IntWritable temp = key.temp ;
			
			
			Iterator<NullWritable>iterator=values.iterator();
		
		while	(iterator.hasNext()){
			   
				context.write(year, temp);
				iterator.next();
				
		}
	}
}
