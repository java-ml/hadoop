package sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 组合key大小对比
 */
public class CompKeyComparator extends WritableComparator {
	
	public CompKeyComparator(){
		super(CompKey.class,true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		
		CompKey k1 = (CompKey)a ;
		CompKey k2 = (CompKey)b ;
		
		int y1 = Integer.parseInt(k1.year.toString());
		int y2 = Integer.parseInt(k2.year.toString());
		
		
		int t1 = k1.temp.get();
		int t2 = k2.temp.get();
		
		if(y1 != y2){
			return y1 - y2 ; 
		}
		return -(t1 - t2) ;
	
	}
		
}
