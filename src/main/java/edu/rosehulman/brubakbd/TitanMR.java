package edu.rosehulman.brubakbd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TitanMR extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		  int res = ToolRunner.run(conf, new edu.rosehulman.brubakbd.TitanMR(), args);
		  System.exit(res);
		
//		if (args.length != 1){
//			System.err.println("Usage: TitanMR <input path>");
//			System.exit(-1);
//		}
		
		
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(super.getConf());
		job.setJarByClass(TitanMR.class);
		job.setJobName("TitanMR");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputFormatClass(NullOutputFormat.class);
		
		job.setMapperClass(TitanMRMapper.class);
		job.setNumReduceTasks(0);
		
		System.out.println("about to submit job");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}
