package com.zikesjan.bigdata.formatingmatrix;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer performing the final Tf-Idf matrix rotation
 * @author zikesjan
 *
 */
public class RotateReducer extends Reducer<Text, Text, Text, Text>{

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException{
				
		StringBuilder sb = new StringBuilder();
        for (Text val : values) {
            sb.append(val.toString()).append(" ");
        }
        sb.deleteCharAt(sb.length()-1);
        context.write(new Text(key.toString()), new Text(sb.toString()));
        
	}
	
}
