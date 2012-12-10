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

public class InvertedIndex {
	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, value);
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text k, Iterator<Text> vs,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			HashSet<String> set = new HashSet<String>();
			while (vs.hasNext()) {
				set.add(vs.next().toString());
			}
			String ret = join(1.0f, set);
			output.collect(k, new Text(ret));
		}

	}

	private static String join(float mass, HashSet<String> set) {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("%.8f", mass));
		for (String s : set) {
			sb.append("$$$");
			sb.append(s);
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("InvertedIndex");

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// conf.set("key.value.separator.in.input.line", "\t");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
