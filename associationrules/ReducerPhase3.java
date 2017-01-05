package associationrules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ReducerPhase3 extends Reducer<Text, Text, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<String> idA = new ArrayList<String>(), idB = new ArrayList<String>();
		ArrayList<Integer> countAtoB = new ArrayList<>();
		float countA = 1;
		
		for (Text value : values) {
			String val = value.toString();
			StringTokenizer tok = new StringTokenizer(val, "/");
			if (tok.countTokens() == 1) {
				countA = Float.parseFloat(tok.nextToken());
			} else {
				StringTokenizer tokKey = new StringTokenizer(tok.nextToken(), "_");
				countAtoB.add(Integer.parseInt(tok.nextToken())); 
				idA.add(tokKey.nextToken());
				idB.add(tokKey.nextToken());
			}
		}
		
		for (int i=0; i<countAtoB.size(); ++i) {
			float sup = countAtoB.get(i) / countA;
			if (sup > 0.7) {
				context.write(new Text(idA.get(i) + "->" + idB.get(i)), new FloatWritable(sup));
			}
		}
		
		cleanup(context);
	}

}
