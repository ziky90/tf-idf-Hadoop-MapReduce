package com.zikesjan.bigdata.cleaningcounting;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zikesjan.bigdata.TfIdfMain.MyCounters;


public class FrequencyReducer extends Reducer<WordDocumentWritable, Text, Text, Text>{
	
	private HashSet<String> documents;
	
	public void setup(Context context) throws IOException, InterruptedException {
		documents = new HashSet<String>();
	}
	
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
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.getCounter(MyCounters.Documents).increment(documents.size());
	}
}
