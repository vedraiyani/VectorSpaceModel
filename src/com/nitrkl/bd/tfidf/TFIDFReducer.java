package com.nitrkl.bd.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

public class TFIDFReducer extends Reducer<Text, DocumentFrequencyValue, Text, Text> {
	public static int TOTAL_IP_DOCS = 0;

	public void reduce(Text _key, Iterable<DocumentFrequencyValue> values, Context context) throws IOException, InterruptedException {
		// process values
		Set<Text> documents = new HashSet<>();
		Iterable<DocumentFrequencyValue> valuesIterable=values;
		ArrayList<DocumentFrequencyValue> l=new ArrayList<>();
		for (DocumentFrequencyValue val : values) {
			documents.add(val.getDocument());
			l.add(val);
		}
		
		
		double idf=Math.log10((double)TOTAL_IP_DOCS / documents.size());
		for (DocumentFrequencyValue val : l) {
			double tfidf= Double.parseDouble(val.getFrequency().toString())*idf;
			context.write(_key,new Text(val.getDocument()+"\t"+tfidf));
		}
		// idf of term
		context.write(_key, new Text(documents.size() + " " +idf));
	}

}
