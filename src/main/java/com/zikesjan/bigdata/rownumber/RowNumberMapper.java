package com.zikesjan.bigdata.rownumber;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zikesjan.bigdata.TfIdfMain;

public class RowNumberMapper extends Mapper<LongWritable, Text, ByteWritable, RowNumberWritable>{
	
	private long[] counters;
	private int numReduceTasks;

	private RowNumberWritable outputValue = new RowNumberWritable();
	private ByteWritable outputKey = new ByteWritable();
	
	public void setup(Context context) throws IOException, InterruptedException {
		numReduceTasks = context.getNumReduceTasks();
		counters = new long[numReduceTasks];
		outputKey.set(TfIdfMain.VALUE_MARKER);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		outputValue.setValue(value);
		context.write(outputKey, outputValue);
		counters[RowNumberWritable.Partitioner.partitionForValue(outputValue, numReduceTasks)]++;
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		outputKey.set(TfIdfMain.COUNTER_MARKER);
		for(int c = 0; c < counters.length - 1; c++) {
			if (counters[c] > 0) {
				outputValue.setCounter(c + 1, counters[c]);
				context.write(outputKey, outputValue);
			}
			counters[c + 1] += counters[c];
		}
	}
}
