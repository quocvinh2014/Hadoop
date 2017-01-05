package associationrules;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPhase1 extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringTokenizer tok = new StringTokenizer(value.toString(), ",");
		// skip first value because it's a transaction id
		if (tok.hasMoreElements()) {
			tok.nextToken();
		}
		while (tok.hasMoreElements()) {
			context.write(new Text(tok.nextToken().trim()), new LongWritable(1));
		}
		cleanup(context);
	}

}
