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

public class PageRank {
	private static String pat = "m:%.8f";
	private final static int N = 10000000;
	private final static float d = 0.85f; // damping factor

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (key.toString().trim().length() <= 0)
				return;
			// System.err.format("%s, %s\n", key.toString(), value.toString());
			String[] adj = value.toString().split("[$$$]");
			float mass = Float.parseFloat(adj[0]);
			output.collect(key, value); // recover graph
			for (int i = 1; i < adj.length; i++) {
				output.collect(
						key,
						new Text(String.format(pat, mass
								/ ((float) (adj.length - 1)))));
			}

		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text k, Iterator<Text> vs,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			float mass = (1 - d) / N;
			String adjCol = null;
			while (vs.hasNext()) {
				String v = vs.next().toString();
				if (v.startsWith("m:")) {
					mass += Float.parseFloat(v.substring(2)) * d;
				} else {
					adjCol = v;
				}
			}
			if (adjCol != null) {
				String[] adjs = adjCol.split(pat);
				String ret = join(mass, adjs);
				output.collect(k, new Text(ret));
				System.out.println(k.toString() + adjCol);
			}

		}

	}

	private static String join(float mass, String[] adjs) {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("%.8f", mass));
		for (int i = 1; i < adjs.length; i++) {
			sb.append("$$$");
			sb.append(adjs[i]);
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		// for (int i = 0; i < 5; i++) {
		JobConf conf = new JobConf(PageRank.class);
		String input = args[0], output = args[1];
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setJobName("Pagerank");
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);
		// input = output;
		// output = output + '0';
		// }
	}
}
