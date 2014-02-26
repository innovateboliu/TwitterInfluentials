package ranking.retweetcount;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalRankingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private PriorityQueue<Pair<String, Integer>> pq;
	
	@Override 
	public void setup(Context context){
		pq = new PriorityQueue<Pair<String,Integer>>(TwitterInfluentialRetweetCountDriver.K, new Comparator<Pair<String,Integer>>(){
			public int compare(Pair<String, Integer> f1, Pair<String, Integer> f2) {  
                return f1.second - f2.second;  
            }
		});
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException{
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		pq.add(new Pair<String, Integer>(key.toString(), sum));
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
