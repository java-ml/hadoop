package sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义按照year进行分区
 */
public class YearPartitioner extends Partitioner<CompKey, NullWritable> {

	public int getPartition(CompKey key, NullWritable value, int numPartitions) {
		String year = key.year.toString();
		return year.hashCode() % numPartitions;
	}
}
