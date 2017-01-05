package associationrules;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPhase3 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String val = value.toString();
		StringTokenizer tok = new StringTokenizer(val, "\t");
		String oldKey, weight;
		if (tok.countTokens() == 2) {
			oldKey = tok.nextToken();
			weight = tok.nextToken();
			StringTokenizer tok2 = new StringTokenizer(oldKey, "_");
			String newKey = tok2.nextToken();
			if (tok2.countTokens() > 0) {
				context.write(new Text(newKey), new Text(oldKey + "/" + weight));
			} else {
				context.write(new Text(newKey), new Text(weight));
			}
		}
		cleanup(context);
	}

}
