package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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

public class CoOccurence {

	private static String format = "%s|%d,";

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private int MAX_NEIGHBORS = 30;

		private String pat = "[^A-Za-z0-9]+";
		private Splitter sp = Splitter.onPattern(pat).omitEmptyStrings();

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String str = value.toString().toLowerCase();
			Iterable<String> strs = sp.split(str);
			List<String> wordList = new ArrayList<String>();
			
			for (String s : strs)
				if (s.length() > 1 && s.length() < 50)
					wordList.add(s);
			
			for (int i = 0; i < wordList.size(); i++) {
				String x = wordList.get(i);
				Text k = new Text(x);
				for (int j = i + 1; j < Math.min(wordList.size(), i
						+ MAX_NEIGHBORS); j++) {
					String y = wordList.get(j);
					if (x.compareTo(y) >= 0) {
						String t = x;
						x = y;
						y = t;
					}
					Text v = new Text(String.format(format, y, 1));
					output.collect(k, v);
				}
			}

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
			for (Entry<String, Integer> entry : map.entrySet()) {
				sb.append(entry.getKey());
				sb.append('|');
				sb.append(entry.getValue());
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
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}
