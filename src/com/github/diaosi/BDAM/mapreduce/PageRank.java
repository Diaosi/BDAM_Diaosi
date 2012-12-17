package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {

	private final static int N = 12873488;
	private final static float d = 0.85f; // damping factor

	private static class MapperStep1 extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String k = key.toString().trim();
			if (k.length() <= 0)
				return;
			String v = key.toString().trim();
			if (v.length() <= 0)
				return;
			output.collect(new Text(k), new Text(v));
		}
	}

	private static class CombinerStep1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append(values.next().toString());
				sb.append('\t');
			}
			output.collect(key, new Text(sb.toString()));
		}
	}

	private static class ReducerStep1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private float initPR = 1.0f;

		@Override
		public void reduce(Text k, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append(values.next().toString());
			}
			output.collect(k,
					new Text(String.format("%.8f\t%s", initPR, sb.toString())));
		}

	}

	private static class MapperStep2 extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = value.toString().split("\\t");

		}

	}

	private static class ReducerStep2 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private float initPR = 1.0f;

		@Override
		public void reduce(Text k, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append(values.next().toString());
			}
			output.collect(k,
					new Text(String.format("%.8f\t%s", initPR, sb.toString())));
		}

	}

	//
	// private static String join(float mass, String[] adjs) {
	// StringBuffer sb = new StringBuffer();
	// sb.append(String.format("%.8f", mass));
	// for (int i = 1; i < adjs.length; i++) {
	// sb.append("$$$");
	// sb.append(adjs[i]);
	// }
	// return sb.toString();
	// }

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setMapperClass(MapperStep1.class);
		conf.setCombinerClass(CombinerStep1.class);
		conf.setReducerClass(ReducerStep1.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setJobName("Pagerank");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		RunningJob step1 = JobClient.runJob(conf);
		step1.waitForCompletion();
		// input = output;
		// output = output + '0';
		// }
	}
}
