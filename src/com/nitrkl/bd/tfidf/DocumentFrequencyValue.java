package com.nitrkl.bd.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DocumentFrequencyValue implements Writable {
	private Text document;
	private Text frequency;
	public DocumentFrequencyValue() {
		document=new Text();
		frequency=new Text();
	}
	public DocumentFrequencyValue(Text document, Text frequency) {
		super();
		this.document = document;
		this.frequency = frequency;
	}
	public DocumentFrequencyValue(String document, String frequency) {
		super();
		this.document = new Text(document);
		this.frequency = new Text(frequency);
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
	public Text getFrequency() {
		return frequency;
	}
	public void setFrequency(Text frequency) {
		this.frequency = frequency;
	}
	public void setFrequencyAsString(String frequency) {
		this.frequency = new Text(frequency);
	}
	public void write(DataOutput out) throws IOException {
		frequency.write(out);
		document.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		frequency.readFields(in);
		document.readFields(in);
	}

	public int compareTo(DocumentFrequencyValue o) {
		if(frequency.compareTo(o.frequency) == 0) return document.compareTo(o.document);
		else return frequency.compareTo(o.frequency);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((document == null) ? 0 : document.hashCode());
		result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DocumentFrequencyValue other = (DocumentFrequencyValue) obj;
		if (document == null) {
			if (other.document != null)
				return false;
		} else if (!document.equals(other.document))
			return false;
		if (frequency == null) {
			if (other.frequency != null)
				return false;
		} else if (!frequency.equals(other.frequency))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return frequency + "\t" + document;
	}

}
