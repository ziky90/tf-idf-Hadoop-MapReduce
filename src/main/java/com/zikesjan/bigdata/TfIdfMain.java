package com.zikesjan.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.zikesjan.bigdata.cleaningcounting.FrequencyMapper;
import com.zikesjan.bigdata.cleaningcounting.FrequencyReducer;
import com.zikesjan.bigdata.cleaningcounting.WordDocumentWritable;
import com.zikesjan.bigdata.formatingmatrix.RotateMapper;
import com.zikesjan.bigdata.formatingmatrix.RotateReducer;
import com.zikesjan.bigdata.rownumber.RowNumberMapper;
import com.zikesjan.bigdata.rownumber.RowNumberReducer;
import com.zikesjan.bigdata.rownumber.RowNumberWritable;
import com.zikesjan.bigdata.tfidf.TFIDFMapper;
import com.zikesjan.bigdata.tfidf.TFIDFReducer;
import com.zikesjan.bigdata.tfidf.WordAndStatWritable;

/**
 * main class of the tf-idf implementation. It shows how it is possible to use job chaining and counters in Hadoop Map Reduce.
 * @author zikesjan
 *
 */
public class TfIdfMain {

	public final static byte COUNTER_MARKER = (byte) 'T';
	public final static byte VALUE_MARKER = (byte) 'W';
	
	//addresses of the helping storage directories, just to see what's going on after each MapReduce jobs for IBM
    private static final String OUTPUT_PATH = "/user/biadmin/output/ordered";
    private static final String OUTPUT_PATH_2 = "/user/biadmin/output/total";
    private static final String OUTPUT_PATH_3 = "/user/biadmin/output/tfidfresult";
    
    //addresses of the helping storage directories jobs for Cloudera
    /*private static final String OUTPUT_PATH = "/user/cloudera/wiki/ordered";
    private static final String OUTPUT_PATH_2 = "/user/cloudera/wiki/total";
    private static final String OUTPUT_PATH_3 = "/user/cloudera/wiki/tfidfresult";*/

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		//input and output path that is passed in arguments
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create linecounting job
		Job countLines = new Job(conf, "CountLines");
		countLines.setJarByClass(RowNumberMapper.class);
		countLines.setMapperClass(RowNumberMapper.class);
		countLines.setMapOutputKeyClass(ByteWritable.class);
		countLines.setMapOutputValueClass(RowNumberWritable.class);
		countLines.setReducerClass(RowNumberReducer.class);
		countLines.setNumReduceTasks(1);
		countLines.setOutputKeyClass(Text.class);
		countLines.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(countLines, inputPath);
		countLines.setInputFormatClass(TextInputFormat.class);
		Path countedPath = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(countLines, countedPath);
		countLines.setOutputFormatClass(TextOutputFormat.class);
		
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(countedPath))
			hdfs.delete(countedPath, true);

		// Execute the line counting job
		int code = countLines.waitForCompletion(true) ? 0 : 1;

		// Create frequency counting job
		Job frequencyCounting = new Job(conf, "WordPerDocument");
		frequencyCounting.setJarByClass(FrequencyMapper.class);
		frequencyCounting.setMapperClass(FrequencyMapper.class);
		frequencyCounting.setMapOutputKeyClass(WordDocumentWritable.class);
		frequencyCounting.setMapOutputValueClass(Text.class);
		frequencyCounting.setReducerClass(FrequencyReducer.class);
		frequencyCounting.setOutputKeyClass(Text.class);
		frequencyCounting.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(frequencyCounting, countedPath);
		frequencyCounting.setInputFormatClass(KeyValueTextInputFormat.class);
		Path finalStatisticsPath = new Path(OUTPUT_PATH_2);
		FileOutputFormat.setOutputPath(frequencyCounting, finalStatisticsPath);
		frequencyCounting.setOutputFormatClass(TextOutputFormat.class);
		
		// Delete output if exists
		if (hdfs.exists(finalStatisticsPath))
			hdfs.delete(finalStatisticsPath, true);
		
		// Execute frequency counting job
		code = frequencyCounting.waitForCompletion(true) ? 0 : 1;
		//getting the number of rows counter's value
		long documents = frequencyCounting.getCounters().findCounter(MyCounters.Documents).getValue();
		
		//Create final tfidf computing job
		Configuration tfidfConf = new Configuration();
		//passing the number of rows to the tfidf MapReduce
		tfidfConf.set("documents", documents+"");	
		Job tfidf = new Job(tfidfConf, "RFIDF");
		tfidf.setJarByClass(TFIDFMapper.class);
		tfidf.setMapperClass(TFIDFMapper.class);
		tfidf.setMapOutputKeyClass(Text.class);
		tfidf.setMapOutputValueClass(WordAndStatWritable.class);
		tfidf.setReducerClass(TFIDFReducer.class);
		tfidf.setOutputKeyClass(Text.class);
		tfidf.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(tfidf, finalStatisticsPath);
		tfidf.setInputFormatClass(KeyValueTextInputFormat.class);
		Path reformatedPath = new Path(OUTPUT_PATH_3);
		FileOutputFormat.setOutputPath(tfidf, reformatedPath);
		tfidf.setOutputFormatClass(TextOutputFormat.class);
		
		if (hdfs.exists(reformatedPath))
			hdfs.delete(reformatedPath, true);
		
		//execute the tfidf job
		code = tfidf.waitForCompletion(true) ? 0 : 1;
		
		//Create rotate matrix job
		Job rotateMatrix = new Job(tfidfConf, "Rotate");
		rotateMatrix.setJarByClass(RotateMapper.class);
		rotateMatrix.setMapperClass(RotateMapper.class);
		rotateMatrix.setMapOutputKeyClass(Text.class);
		rotateMatrix.setMapOutputValueClass(Text.class);
		rotateMatrix.setReducerClass(RotateReducer.class);
		rotateMatrix.setNumReduceTasks(1);
		rotateMatrix.setOutputKeyClass(Text.class);
		rotateMatrix.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(rotateMatrix, reformatedPath);
		rotateMatrix.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(rotateMatrix, outputDir);
		rotateMatrix.setOutputFormatClass(TextOutputFormat.class);
		
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		//executing the matrix rotation job
		code = rotateMatrix.waitForCompletion(true) ? 0 : 1;
		
		System.exit(code);
	}
	
	/**
	 * counters definition
	 * @author zikesjan
	 *
	 */
	public enum MyCounters {
		Documents
	}
}
