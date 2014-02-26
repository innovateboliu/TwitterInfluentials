package com.bo.ranking.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class NameScoreKey implements WritableComparable<NameScoreKey>{
	private String name;
	private float score;
	
	public NameScoreKey() {}
	public NameScoreKey(String name, float score) {
		this.name = name;
		this.score = score;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = WritableUtils.readString(in);
		this.score = in.readFloat();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, this.name);
		out.writeFloat(this.score);
	}
	@Override
	public int compareTo(NameScoreKey o) {
		return name.compareTo(o.name);
	}
	
	@Override
	public String toString() {
		return this.name+":"+this.score;
	}
}
