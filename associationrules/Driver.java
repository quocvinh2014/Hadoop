package associationrules;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Path input, output;
		if (args.length > 1) {
			input = new Path(args[0]);
			output = new Path(args[1]);
		} else {
			input = new Path("input");
			output = new Path("output");
		}
		
		// start phase 1
		Path k1 = new Path("k1");
		Job jobPhase1 = Job.getInstance(conf);
		jobPhase1.setJarByClass(Driver.class);
		jobPhase1.setJobName("Step 1");
		
		FileInputFormat.setInputPaths(jobPhase1, input);
		FileOutputFormat.setOutputPath(jobPhase1, k1);
		jobPhase1.setOutputKeyClass(Text.class);
		jobPhase1.setOutputValueClass(LongWritable.class);
		jobPhase1.setMapperClass(MapperPhase1.class);
		jobPhase1.setReducerClass(ReducerPhase1.class);
		
		boolean success = jobPhase1.waitForCompletion(true);
		// end of phase 1
		
		// start phase 2
		Job jobPhase2 = Job.getInstance(conf);
		Path k2 = new Path("k2");
		FileInputFormat.setInputPaths(jobPhase2, input);
		FileOutputFormat.setOutputPath(jobPhase2, k2);
		jobPhase2.setMapperClass(MapperPhase2.class);
		jobPhase2.setReducerClass(ReducerPhase2.class);
		jobPhase2.setOutputKeyClass(Text.class);
		jobPhase2.setOutputValueClass(LongWritable.class);
		success = jobPhase2.waitForCompletion(true);
		
		Job jobPhase3 = Job.getInstance(conf);
		FileInputFormat.setInputPaths(jobPhase3, k1, k2);
		FileOutputFormat.setOutputPath(jobPhase3, output);
		jobPhase3.setMapperClass(MapperPhase3.class);
		jobPhase3.setReducerClass(ReducerPhase3.class);
		jobPhase3.setOutputKeyClass(Text.class);
		jobPhase3.setOutputValueClass(Text.class);
		success = jobPhase3.waitForCompletion(true);
		
		System.exit(success ? 1 : 0);
	}

}
