package com.zikesjan.bigdata.cleaningcounting;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<WordDocumentWritable, Text, Text, Text>{
	
	public void reduce(WordDocumentWritable key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException{
		int sum = 0;
		int total = Integer.parseInt(values.iterator().next().toString());
        for (Text tx : values) {
            sum ++;
        }
        if(sum > 5) context.write(new Text(key.toString()), new Text(sum+" "+total));
        
	}
}
