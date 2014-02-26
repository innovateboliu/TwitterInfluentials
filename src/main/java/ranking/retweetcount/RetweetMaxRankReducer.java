package ranking.retweetcount;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RetweetMaxRankReducer extends Reducer<CompositeKey, CompositeValue, Text, IntWritable> {
	private PriorityQueue<Pair<String, Integer>> pq;
	
	@Override
	public void setup(Context context) {
		pq = new PriorityQueue<Pair<String,Integer>>(TwitterInfluentialRetweetCountDriver.K, new Comparator<Pair<String,Integer>>(){
			public int compare(Pair<String, Integer> f1, Pair<String, Integer> f2) {  
                return f1.second - f2.second;  
            }
		});
	}
	
	@Override
	public void reduce(CompositeKey key, Iterable<CompositeValue> values, Context context) {
		int maxRetweetCount = 0;
		int count = 0;
		CompositeValue preValue = new CompositeValue();

		for (CompositeValue value : values) {
			if (value.tweetId == preValue.tweetId) {
				maxRetweetCount = Math.max(maxRetweetCount, value.count);
			} else {
				count += maxRetweetCount;
				maxRetweetCount = value.count;
			}
			preValue = value;
		}
		count += maxRetweetCount;
		
		pq.add(new Pair<String, Integer>(key.pair.first, count));
		if (pq.size() > TwitterInfluentialRetweetCountDriver.K) {
			pq.remove();
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Pair<String, Integer> pair : pq) {
			context.write(new Text(pair.first), new IntWritable(pair.second));
		}
	}
}

