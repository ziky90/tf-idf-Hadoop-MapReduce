package com.zikesjan.bigdata.rownumber;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer class calculating the lines and writing them out
 * @author zikesjan
 *
 */
public class RowNumberReducer extends Reducer<ByteWritable, RowNumberWritable, Text, Text>{
	
	private Text outputKey = new Text();
	private long numberOfRows;

	/**
	 * method that determines line number and writes this number as key and the line as value.
	 */
	public void reduce(ByteWritable key, Iterable<RowNumberWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<RowNumberWritable> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		long offset = 0;
		RowNumberWritable value = itr.next();
		while (itr.hasNext() && value.getCount() > 0) {
			offset += value.getCount();
			value = itr.next();
		}
		outputKey.set(Long.toString(offset++));
		context.write(outputKey, value.getValue());

		while(itr.hasNext()) {
			value = itr.next();
			if(value.getValue() != null){
				outputKey.set(Long.toString(offset++));
				context.write(outputKey, value.getValue());
			}
		}
		if(offset > numberOfRows){
			numberOfRows = offset;
		}
	}
	
	
	
}
