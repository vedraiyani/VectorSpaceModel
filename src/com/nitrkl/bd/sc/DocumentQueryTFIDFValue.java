package com.nitrkl.bd.sc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentQueryTFIDFValue implements Writable {
	private Text document;
	private Text tfidfmul;
	public DocumentQueryTFIDFValue() {
		document=new Text();
		tfidfmul=new Text();
	}
	public DocumentQueryTFIDFValue(Text document, Text tfidfmul) {
		super();
		this.document = document;
		this.tfidfmul = tfidfmul;
	}
	public DocumentQueryTFIDFValue(String document, String tfidfmul) {
		super();
		this.document = new Text(document);
		this.tfidfmul = new Text(tfidfmul);
	}
	public Text getDocument() {
		return document;
	}
	public void setDocument(Text document) {
		this.document = document;
	}
	public void setDocumentAsString(String document) {
		this.document = new Text(document);
	}
	public Text getTFIDFMul() {
		return tfidfmul;
	}
	public void setTFIDFMul(Text tfidfmul) {
		this.tfidfmul = tfidfmul;
	}
	public void setTFIDFMulAsString(String tfidfmul) {
		this.tfidfmul = new Text(tfidfmul);
	}
	public void write(DataOutput out) throws IOException {
		tfidfmul.write(out);
		document.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		tfidfmul.readFields(in);
		document.readFields(in);
	}

//	public int compareTo(DocumentFrequencyValue o) {
//		if(frequency.compareTo(o.frequency) == 0) return document.compareTo(o.document);
//		else return frequency.compareTo(o.frequency);
//	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((document == null) ? 0 : document.hashCode());
		result = prime * result + ((tfidfmul == null) ? 0 : tfidfmul.hashCode());
		return result;
	}

//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		DocumentFrequencyValue other = (DocumentFrequencyValue) obj;
//		if (document == null) {
//			if (other.document != null)
//				return false;
//		} else if (!document.equals(other.document))
//			return false;
//		if (frequency == null) {
//			if (other.frequency != null)
//				return false;
//		} else if (!frequency.equals(other.frequency))
//			return false;
//		return true;
//	}

	@Override
	public String toString() {
		return tfidfmul + "\t" + document;
	}

}
