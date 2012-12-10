package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class CoOccurence {

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, IntWritable> {

		private static String pat = "[\\~\\`\\!\\@\\#\\$\\%\\^\\&\\*\\(\\)\\_\\-\\+\\=\\|\\\\\\\\}\\]\\{\\[\\:\\;\\'\\\\\"\\?\\/\\>\\.\\<\\,\\ ]";

		private static String format = "%s,%s";

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			IntWritable one = new IntWritable(1);
			String v = value.toString();
			String[] strs = v.split(pat);
			HashSet<String> set = new HashSet<String>();
			for (String s : strs) {
				if (!"".equals(s)) {
					set.add(s);
				}
			}
			String[] strCol = set.toArray(new String[] {});
			for (int i = 0; i < strCol.length; i++)
				for (int j = 0; j < strCol.length; j++)
					if (i != j) {
						String x = strCol[i];
						String y = strCol[j];
						if (x.compareTo(y) <= 0) {
							String k = String.format(format, x, y);
							output.collect(new Text(k), one);
						}

					}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text k, Iterator<IntWritable> vs,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int cnt = 0;
			while (vs.hasNext()) {
				cnt += vs.next().get();
			}
			output.collect(k, new IntWritable(cnt));
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CoOccurence.class);
		conf.setJobName("CountOfPages");
		conf.setMapperClass(MyMapper.class);
		// conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyReducer.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
