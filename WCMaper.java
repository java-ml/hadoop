package sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MR:Map
 */
public class WCMaper extends Mapper<Text, IntWritable, CompKey, NullWritable> {

	@Override
	protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, CompKey, NullWritable>.Context context)
			throws IOException, InterruptedException {
		CompKey ck = new CompKey();
		ck.year = key ;
		ck.temp = value ;
		context.write(ck, NullWritable.get());
	}
}
