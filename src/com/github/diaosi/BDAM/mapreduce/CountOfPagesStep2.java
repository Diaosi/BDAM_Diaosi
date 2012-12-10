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

public class CountOfPagesStep2 {

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, IntWritable> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			output.collect(new Text(""), new IntWritable(1));

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
				IntWritable ii = vs.next();
				cnt += ii.get();
			}
			output.collect(k, new IntWritable(cnt));
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CountOfPagesStep2.class);
		conf.setJobName("CountOfPages1");
		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
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
