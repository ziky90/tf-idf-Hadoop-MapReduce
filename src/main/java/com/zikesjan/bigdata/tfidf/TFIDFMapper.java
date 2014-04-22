package com.zikesjan.bigdata.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<Text, Text, Text, WordAndStatWritable>{

	private WordAndStatWritable wsw = new WordAndStatWritable();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] keySplit = key.toString().split(" ");
		String[] valueSplit = value.toString().split(" ");
		wsw.setDocumentByString(keySplit[1]);
		wsw.setOccurencePerDocumentByString(valueSplit[0]);
		wsw.setTotalOccurenciesByString(valueSplit[1]);
		context.write(new Text(keySplit[0]), wsw);
	}
	
}
