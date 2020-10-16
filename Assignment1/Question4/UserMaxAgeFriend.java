package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserMaxAgeFriend {

	public static void main(String[] args) throws Exception {
		// <input_file1> <input_file2(in memory)> <output_file>

		//	args = new String[] { "./soc-LiveJournal1Adj.txt", "./userdata.txt", "./output" };

		Configuration conf = new Configuration();
		conf.set("userdata", args[1]);
		Job job = Job.getInstance(conf);

		job.setJarByClass(UserMaxAgeFriend.class);
		job.setMapperClass(UserMaxAgeFriendMapper.class);
		job.setReducerClass(UserMaxAgeFriendReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		boolean result = job.waitForCompletion(true);

		System.out.println(result ? 1 : 0);
	}
}
