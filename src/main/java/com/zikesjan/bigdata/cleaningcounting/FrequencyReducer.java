package com.zikesjan.bigdata.cleaningcounting;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zikesjan.bigdata.TfIdfMain.MyCounters;

/**
 * Reducer class that sums occurencies and writes count data out back to HDFS
 * @author zikesjan
 *
 */
public class FrequencyReducer extends Reducer<WordDocumentWritable, Text, Text, Text>{
	
	private HashSet<String> documents;
	
	/**
	 * setup method to setup HashSet containing all the documents
	 */
	public void setup(Context context) throws IOException, InterruptedException {
		documents = new HashSet<String>();
	}
	
	/**
	 * reducer method that performs summing and writing
	 */
	public void reduce(WordDocumentWritable key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException{
		int sum = 0;
		int total = Integer.parseInt(values.iterator().next().toString());
        for (Text tx : values) {
            sum ++;
        }
        if(sum > 5) {
        	context.write(new Text(key.toString()), new Text(sum+" "+total));
        	documents.add(key.toString().split(" ")[1]);
        }
	}
	
	/**
	 * Usage of the counters in cleanup method
	 */
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.getCounter(MyCounters.Documents).increment(documents.size());
	}
}
