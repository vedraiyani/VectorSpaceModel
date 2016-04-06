package com.nitrkl.bd.sc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SCReducer extends Reducer<Text, Text, Text, Text> {
	public static int TOTAL_IP_DOCS = 0;

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sc=0;
		for(Text val:values){
			sc+=Double.parseDouble(val.toString());
		}
		context.write(_key, new Text(sc+""));
	}

}
