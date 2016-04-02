package com.nitrkl.bd.tf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TermDocumentWritable implements WritableComparable<TermDocumentWritable> {
	private Text document;
	private Text term;
	public TermDocumentWritable() {
		document=new Text();
		term=new Text();
	}
	public TermDocumentWritable(Text document, Text term) {
		super();
		this.document = document;
		this.term = term;
	}
	public Text getDocument() {
		return document;
	}
	public void setDocument(Text document) {
		this.document = document;
	}
	public Text getTerm() {
		return term;
	}
	public void setTerm(Text term) {
		this.term = term;
	}
	public void write(DataOutput out) throws IOException {
		term.write(out);
		document.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		term.readFields(in);
		document.readFields(in);
	}

	public int compareTo(TermDocumentWritable o) {
		if(term.compareTo(o.term) == 0) return document.compareTo(o.document);
		else return term.compareTo(o.term);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((document == null) ? 0 : document.hashCode());
		result = prime * result + ((term == null) ? 0 : term.hashCode());
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
		TermDocumentWritable other = (TermDocumentWritable) obj;
		if (document == null) {
			if (other.document != null)
				return false;
		} else if (!document.equals(other.document))
			return false;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return term + " " + document;
	}

}
