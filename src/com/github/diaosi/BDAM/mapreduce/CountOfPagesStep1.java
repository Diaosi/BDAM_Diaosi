package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CountOfPagesStep1 {

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String k = key.toString().trim();
			if (k.length() > 0)
				output.collect(new Text(""), new Text(key));
			String v = value.toString().trim();
			if (v.length() > 0)
				output.collect(new Text(""), new Text(value));

		}
	}

	// private static class MyCombiner extends MapReduceBase implements
	// Reducer<Text, Text, Text, Text> {
	//
	// @Override
	// public void reduce(Text k, Iterator<Text> vs,
	// OutputCollector<Text, Text> output, Reporter reporter)
	// throws IOException {
	// Text key = new Text("");
	// HashSet<String> set = new HashSet<String>();
	// while (vs.hasNext()) {
	// set.add(vs.next().toString());
	// }
	// for (String title : set) {
	// output.collect(key, new Text(title));
	// }
	//
	// }
	//
	// }

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		static HashSet<String> set = new HashSet<String>();

		@Override
		public void reduce(Text k, Iterator<Text> vs,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// Text key = new Text("");
			while (vs.hasNext()) {
				set.add(vs.next().toString());
			}
			for (String title : set) {
				output.collect(k, new Text(title));
			}
			// output.collect(key, new Text("" + set.size()));

		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CountOfPagesStep1.class);
		conf.setJobName("CountOfPages");
		conf.setMapperClass(MyMapper.class);
		// conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyReducer.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
