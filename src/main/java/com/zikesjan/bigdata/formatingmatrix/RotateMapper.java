package com.zikesjan.bigdata.formatingmatrix;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RotateMapper extends Mapper<Text, Text, Text, Text>{

	
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] keySplit = key.toString().split(" ");
		String[] valueSplit = value.toString().split(" ");
		for(String s : valueSplit){
			String[] split = s.split(":");
			if(split.length == 2) context.write(new Text(split[0]), new Text(keySplit[0]+":"+split[1]));
		}
	}
	
}
