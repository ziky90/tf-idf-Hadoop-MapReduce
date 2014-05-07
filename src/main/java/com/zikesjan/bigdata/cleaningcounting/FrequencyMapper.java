package com.zikesjan.bigdata.cleaningcounting;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.cz.CzechStemmer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

/**
 * Frequency mapper class that clens the raw input data and outputs WordDocumentWritable which represents tuple of word and document.
 * @author zikesjan
 *
 */
public class FrequencyMapper extends Mapper<Text, Text, WordDocumentWritable, Text> {


	/**
	 * map method that performs all the logic
	 */
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		String[] csv = value.toString().split(" ");
		Collection<WordDocumentWritable> keys = new LinkedList<WordDocumentWritable>();
		for (String str : csv) {
			String preprocessed = str.toLowerCase();
			//striping the HTML tags
			preprocessed = str.replaceAll("\\<.*?\\>", "");
			//getting rid of wierd characters
			preprocessed = str.replaceAll("[^a-zěščřžýáíéňťď]", "");
			CzechStemmer cs = new CzechStemmer();
			if (preprocessed.length() > 2
					&& !CzechAnalyzer.getDefaultStopSet()
							.contains(preprocessed) && !EnglishAnalyzer.getDefaultStopSet().contains(preprocessed)) {
				int cut = cs.stem(preprocessed.toCharArray(), preprocessed.length());
				preprocessed = preprocessed.substring(0, cut);
				if(!CzechAnalyzer.getDefaultStopSet().contains(preprocessed)){
					WordDocumentWritable wdw = new WordDocumentWritable();
					wdw.setWordByStrings(preprocessed);
					wdw.setDocument(key);
					keys.add(wdw);
				}
			}
		}
		for(WordDocumentWritable wdw : keys){
			context.write(wdw, new Text(keys.size()+""));
		}
	}
}
