package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxFriends {

	public static void main(String[] args) throws Exception {
		// <input_file> <tmp_file> <output_file>

		// args = new String[] { "./soc-LiveJournal1Adj.txt", "./tmp", "./output" };

		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);

			job.setJarByClass(MaxFriends.class);
			job.setMapperClass(MaxFriendsMapper1.class);
			job.setReducerClass(MaxFriendsReducer1.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			boolean result1 = job.waitForCompletion(true);

			if (!result1) {
				System.exit(0);
			}

			{
				Configuration conf2 = new Configuration();
				Job job2 = Job.getInstance(conf2);

				job2.setJarByClass(MaxFriends.class);
				job2.setMapperClass(MaxFriendsMapper2.class);
				job2.setReducerClass(MaxFriendsReducer2.class);

				job2.setMapOutputKeyClass(IntWritable.class);
				job2.setMapOutputValueClass(Text.class);

				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);

				job2.setNumReduceTasks(1);

				FileInputFormat.setInputPaths(job2, new Path(args[1]));
				FileOutputFormat.setOutputPath(job2, new Path(args[2]));

				boolean result2 = job2.waitForCompletion(true);

				System.out.println(result2 ? 1 : 0);
			}
		}
	}
}
