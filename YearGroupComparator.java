package sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * 按照year分组Key
 */
public class YearGroupComparator extends WritableComparator {
	
	private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
	
	public YearGroupComparator() {
		super(CompKey.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		CompKey k1 = (CompKey) a;
		CompKey k2 = (CompKey) b;
		
		int y1 = Integer.parseInt(k1.year.toString());
		int y2 = Integer.parseInt(k2.year.toString());
		return y1 - y2;
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		
		try {
			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
			int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
			return 1;// TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
