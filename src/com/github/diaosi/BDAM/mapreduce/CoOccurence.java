package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Pattern;

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

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CoOccurence {

	private static String format = "%s|%d,";

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private String pat = "[^A-Za-z0-9]+";
		private Splitter sp = Splitter.onPattern(pat).omitEmptyStrings();

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String v = value.toString().toLowerCase();
			Iterable<String> strs = sp.split(v);
			HashSet<String> set = Sets.newHashSet();
			for (String s : strs) {
				if (s.length() > 0) {
					set.add(s.toLowerCase());
				}
			}

			String[] strCol = set.toArray(new String[] {});
			set.clear();
			set = null;
			for (int i = 0; i < strCol.length; i++)
				if (strCol[i].length() > 0) {
					StringBuilder sb = new StringBuilder();
					String x = strCol[i];
					for (int j = 0; j < strCol.length; j++)
						if (i != j) {
							String y = strCol[j];
							if (x.compareTo(y) <= 0) {
								sb.append(String.format(format, y, 1));
							}
						}
					if (sb.length() > 0) {
						output.collect(new Text(x), new Text(sb.toString()));
						// System.err.format("%d, %d", x, sb.toString());
					}
					sb = null;
				}
			strCol = null;
			strs = null;
			v = null;
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Pattern pat1 = Pattern.compile(",");
		private Pattern pat2 = Pattern.compile("\\|");

		@Override
		public void reduce(Text k, Iterator<Text> vs,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashMap<String, Integer> map = Maps.newHashMap();
			while (vs.hasNext()) {
				Text value = vs.next();
				String pairs[] = pat1.split(value.toString());
				for (String pair : pairs) {
					String[] s = pat2.split(pair);
					String key = s[0];
					int count = Integer.parseInt(s[1]);
					Integer cur = map.get(key);
					if (cur != null) {
						count += cur;
					}
					map.put(key, count);
				}
			}
			StringBuffer sb = new StringBuffer();
			for (String key : map.keySet()) {
				sb.append(key);
				sb.append('|');
				sb.append(map.get(key));
				sb.append(',');
			}
			output.collect(k, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CoOccurence.class);
		conf.setJobName("Co-occurence");
		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.set("mapred.map.child.java.opts", "-Xmx1024m");
		conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
