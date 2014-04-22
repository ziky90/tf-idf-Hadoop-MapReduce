package com.zikesjan.bigdata.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TFIDFReducer extends Reducer<Text, WordAndStatWritable, Text, Text>{

	public void reduce(Text key, Iterable<WordAndStatWritable> values, Context context) throws IOException,
    InterruptedException{
		Configuration conf = context.getConfiguration();
		long totalDocuments = Long.parseLong(conf.get("documents"));				
		int numberOfDocumentsWithKey = 0;
		HashMap<String, List<String>> frequencies = new HashMap<String, List<String>>();
		for(WordAndStatWritable val : values){
			if(Integer.parseInt(val.getOccurencePerDocument().toString()) > 0) numberOfDocumentsWithKey++;
			List<String> l = new ArrayList<String>();
			l.add(val.getOccurencePerDocument().toString());
			l.add(val.getWordsInDocument().toString());
			frequencies.put(val.getDocument().toString(), l);
		}
		
		StringBuilder result = new StringBuilder();
		for(String doc : frequencies.keySet()){
			double tf = Double.valueOf(frequencies.get(doc).get(0)) / Double.valueOf(frequencies.get(doc).get(1));
			double idf = Math.log((double) totalDocuments / (double) (numberOfDocumentsWithKey+1));
			double tfidf = tf*idf;
			result.append(" "+doc+":"+tfidf);
		}
		context.write(key, new Text(result.toString()));
	}
	
}
