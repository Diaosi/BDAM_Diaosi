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
			String v = value.toString().trim();
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
				sb.append('@');
			}
			sb.delete(sb.length() - 1, sb.length());
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
					new Text(String.format("%.8f@%s", initPR, sb.toString())));
		}

	}

	private static class MapperStep2 extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] tokens = value.toString().split("@");
			float PR = Float.parseFloat(tokens[0]);
			output.collect(key, new Text("ST:" + value.toString()));
			int cnt = tokens.length - 1;
			for (int i = 1; i < tokens.length; i++) {
				output.collect(new Text(tokens[i]),
						new Text(String.format("PR:%.8f", PR / ((float) cnt))));
			}

		}
	}

	private static class ReducerStep2 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text k, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String cons = null;
			float total = 0.15f / N;

			while (values.hasNext()) {
				String line = values.next().toString();
				if (line.startsWith("ST:")) {
					int pos = line.indexOf('@');
					if (pos != -1) {
						cons = line.substring(0, pos);
					}
				} else {
					float cur = Float.parseFloat(line.substring(3));
					total += cur;
				}

			}
			String ret = String.format("%.8f@", total);
			if (cons != null)
				ret += cons;
			output.collect(k, new Text(ret));

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf step0 = new JobConf(PageRank.class);

		step0.setMapperClass(MapperStep1.class);
		step0.setCombinerClass(CombinerStep1.class);
		step0.setReducerClass(ReducerStep1.class);

		step0.setInputFormat(KeyValueTextInputFormat.class);
		step0.setOutputFormat(TextOutputFormat.class);

		step0.setOutputKeyClass(Text.class);
		step0.setOutputValueClass(Text.class);

		step0.setMapOutputKeyClass(Text.class);
		step0.setMapOutputValueClass(Text.class);

		step0.setJobName("Pagerank Step0");
		FileInputFormat.setInputPaths(step0, new Path(args[0]));
		FileOutputFormat.setOutputPath(step0, new Path(args[1] + "/0/"));
		RunningJob step1RJ = JobClient.runJob(step0);
		step1RJ.waitForCompletion();
		int i;
		for (i = 1; i < 10; i++) {
			JobConf step2 = new JobConf(PageRank.class);
			step2.setMapperClass(MapperStep2.class);
			step2.setReducerClass(ReducerStep2.class);

			step2.setOutputKeyClass(Text.class);
			step2.setOutputValueClass(Text.class);

			step2.setMapOutputKeyClass(Text.class);
			step2.setMapOutputValueClass(Text.class);

			step2.setInputFormat(KeyValueTextInputFormat.class);
			step2.setOutputFormat(TextOutputFormat.class);

			step2.setJobName("Pagerank Step" + i);
			FileInputFormat.setInputPaths(step2, new Path(args[1] + "/"
					+ (i - 1) + "/"));
			FileOutputFormat.setOutputPath(step2, new Path(args[1] + "/" + (i)
					+ "/"));
			RunningJob step2RJ = JobClient.runJob(step2);
			step2RJ.waitForCompletion();
		}
		// sort pageranks
		JobConf sort = new JobConf(SortRank.class);
		sort.setMapperClass(SortRank.Map.class);

		sort.setMapOutputKeyClass(Text.class);
		sort.setMapOutputValueClass(Text.class);

		sort.setInputFormat(KeyValueTextInputFormat.class);
		sort.setOutputFormat(TextOutputFormat.class);

		sort.setJobName("Sort Pagerank");
		FileInputFormat.setInputPaths(sort, new Path(args[1] + "/" + (i - 1)
				+ "/"));
		FileOutputFormat.setOutputPath(sort, new Path(args[1] + "/sorted/"));
		RunningJob sortRJ = JobClient.runJob(sort);
		sortRJ.waitForCompletion();

		// input = output;
		// output = output + '0';
		// }
	}
}
