import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class AccessLogCount extends Configured implements Tool {

	private static int convertMonthStringToInt(String monthString) {
		switch(monthString.toLowerCase()) {
			case "jan":
				return 1;
			case "feb":
				return 2;
			case "mar":
				return 3;
			case "apr":
				return 4;
			case "may":
				return 5;
			case "jun":
				return 6;
			case "jul":
				return 7;
			case "aug":
				return 8;
			case "sep":
				return 9;
			case "oct":
				return 10;
			case "nov":
				return 11;
			case "dec":
				return 12;
			default:
				return 1;
		}
	}

	private static Date preprocessTimestamp(String timestampWithTimezoneOffset) {
		String timestampWithoutTimezoneOffset = timestampWithTimezoneOffset.split(" ")[0];
		String[] splittedTimestampWithSlash = timestampWithoutTimezoneOffset.split("/");

		int day = Integer.parseInt(splittedTimestampWithSlash[0]);
		int month = convertMonthStringToInt(splittedTimestampWithSlash[1]);

		String[] splittedTimestampWithColon = splittedTimestampWithSlash[2].split(":");
		int year = Integer.parseInt(splittedTimestampWithColon[0]);
		int hour = Integer.parseInt(splittedTimestampWithColon[1]);
		int minute = Integer.parseInt(splittedTimestampWithColon[2]);

		return new Date(year, month, day, hour, minute);
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DateWritableComparable, IntWritable> {
		public void map(LongWritable key, Text value, OutputCollector<DateWritableComparable, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			IntWritable one = new IntWritable(1);
			DateWritableComparable dateWritableComparable = new DateWritableComparable();
			
			int openBracket = line.indexOf("[");
			int closeBracket = line.indexOf("]");

			if (openBracket != -1 && closeBracket != -1) {
				String timestampWithTimezoneOffset = line.substring(line.indexOf("[")+1, line.indexOf("]"));
				dateWritableComparable.setDate(preprocessTimestamp(timestampWithTimezoneOffset));

				output.collect(dateWritableComparable, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<DateWritableComparable, IntWritable, DateWritableComparable, IntWritable> {
		public void reduce(DateWritableComparable key, Iterator<IntWritable> values, OutputCollector<DateWritableComparable, IntWritable> output,
				Reporter reporter) throws IOException {
			int counter = 0;
			while (values.hasNext()) {
				counter += values.next().get();
			}
			output.collect(key, new IntWritable(counter));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, WordCount.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("Access Log Count");

		job.setOutputKeyClass(DateWritableComparable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(DateWritableComparable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		JobClient.runJob(job);

		return 0;
	}	

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: hadoop jar AccessLogCount.jar AccessLogCount <HDFS-INPUT-PATH> <HDFS-OUTPUT-PATH>");
			System.exit(1);
		}

		int res = ToolRunner.run(new Configuration(), new AccessLogCount(), args);
		System.exit(res);
	}
}

