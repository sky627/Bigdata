package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendsNameDate {

	public static void main(String[] args) throws Exception {
		// <input_file1> <tmp_file> <input_file2(in memory)> <userA> <userB> <output_file>

		//	args = new String[] { "./soc-LiveJournal1Adj.txt", "./tmp", "./userdata.txt", "1", "0", "./ouput" };

		{
			Configuration conf1 = new Configuration();
			conf1.set("userA", args[3]);
			conf1.set("userB", args[4]);
			Job job1 = Job.getInstance(conf1);

			job1.setJarByClass(CommonFriendsNameDate.class);
			job1.setMapperClass(CommonFriendsNameDateMapper1.class);
			job1.setReducerClass(CommonFriendsNameDateReducer1.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));

			boolean result1 = job1.waitForCompletion(true);

			if (!result1) {
				System.exit(0);
			}

			{
				Configuration conf2 = new Configuration();
				conf2.set("userdata", args[2]);
				Job job2 = Job.getInstance(conf2);

				job2.setJarByClass(CommonFriendsNameDate.class);
				job2.setMapperClass(CommonFriendsNameDateMapper2.class);

				job2.setMapOutputKeyClass(Text.class);
				job2.setMapOutputValueClass(Text.class);

				FileInputFormat.setInputPaths(job2, new Path(args[1]));
				FileOutputFormat.setOutputPath(job2, new Path(args[5]));

				boolean result2 = job2.waitForCompletion(true);

				System.out.println(result2 ? 1 : 0);
			}
		}
	}
}
