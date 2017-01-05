package associationrules;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase1 extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		String newKey = key.toString() + "_";
		context.write(new Text(newKey), new LongWritable(sum));
		cleanup(context);
	}

}
