package com.zikesjan.bigdata.formatingmatrix;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RotateReducer extends Reducer<Text, Text, Text, Text>{

	/*public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		long totalDocuments = Long.parseLong(conf.get("documents"));
		context.write(new Text("_RowNumbers"), new Text(totalDocuments+""));
	}*/
	
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
