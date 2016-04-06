package com.nitrkl.bd.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, DocumentFrequencyValue, Text, Text> {
	public static int TOTAL_IP_DOCS = 0;

	public void reduce(Text _key, Iterable<DocumentFrequencyValue> values, Context context)
			throws IOException, InterruptedException {

//		Set<String> documents = new HashSet<>();

		ArrayList<DocumentFrequencyValue> documentList = new ArrayList<>();
		
		DocumentFrequencyValue queryTerm=null;
		
		// count document frequency
		for (DocumentFrequencyValue val : values) {
			String document = val.getDocument().toString();
			String frequency = val.getFrequency().toString();
			if (!document.equals("q")) {
				documentList.add(new DocumentFrequencyValue(document, frequency));
//				documents.add(val.getDocument().toString());
			}else{
				queryTerm=new DocumentFrequencyValue(document, frequency);
			}
		}
		
		// count idf
		double idf = Math.log10((double) TOTAL_IP_DOCS / documentList.size());
		
				
		// count and emit tf*idf for query
		double tfidf=0;
		if(queryTerm!=null){
			tfidf = Double.parseDouble(queryTerm.getFrequency().toString()) * idf;
			context.write(_key, new Text(queryTerm.getDocument() + "\t" + tfidf));
		}
		
		// count and emit tf*idf for documents
		for (DocumentFrequencyValue val : documentList) {
			tfidf = Double.parseDouble(val.getFrequency().toString()) * idf;
			context.write(_key, new Text(val.getDocument() + "\t" + tfidf));
		}

		
		
		
		// emit df and idf of term
		// context.write(_key, new Text(documents.size() + " " +idf));
	}

}
