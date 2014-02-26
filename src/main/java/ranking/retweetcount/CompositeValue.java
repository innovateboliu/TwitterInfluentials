package ranking.retweetcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeValue implements Writable{
	protected long tweetId;
	protected int count;
	
	public CompositeValue() {}
	
	public CompositeValue(long tweetId, int count) {
		this.tweetId = tweetId;
		this.count = count;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.tweetId = WritableUtils.readVLong(in);
		this.count = WritableUtils.readVInt(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVLong(out, tweetId);
		WritableUtils.writeVInt(out, count);
	}

}