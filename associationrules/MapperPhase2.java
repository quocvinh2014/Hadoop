package associationrules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPhase2 extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer tok = new StringTokenizer(value.toString(), ",");
		ArrayList<String> list = new ArrayList<String>();
		//skip first element because it's transaction id
		if (tok.hasMoreTokens()) {
			tok.nextToken();
		}
		while (tok.hasMoreElements()) {
			list.add(tok.nextToken().trim());
		}
		if (list.size() == 1) {
			cleanup(context);
			return;
		}
		for (int i = 0; i<list.size()-1; i++) {
			for (int j = i + 1; j < list.size(); ++j) {
				String newKey = String.valueOf(list.get(i)) + "_" + String.valueOf(list.get(j));
				context.write(new Text(newKey), new LongWritable(1));
			}
		}
		cleanup(context);
	}

}
