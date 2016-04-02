package com.nitrkl.bd.tf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<TermDocumentWritable, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	public void reduce(TermDocumentWritable _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(new Text(_key.toString()), result);
	}

}
