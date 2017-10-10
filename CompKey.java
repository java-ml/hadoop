package sort;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 组合key
 */
public class CompKey implements WritableComparable<CompKey>{
	public Text year = new Text();
	public IntWritable temp = new IntWritable();

	public void write(DataOutput out) throws IOException {
		year.write(out);
		temp.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		temp.readFields(in);
	}

	/**
	 * 比较组合key
	 */
	public int compareTo(CompKey o) {
		String y = year.toString();
		int t = temp.get();
		
		String y0 = o.year.toString();
		int t0 = o.temp.get();
		
		if(Integer.parseInt(y) != Integer.parseInt(y0) ){
			return Integer.parseInt(y) - Integer.parseInt(y0) ;
		}
		
		return t - t0 ; 
	}
}
