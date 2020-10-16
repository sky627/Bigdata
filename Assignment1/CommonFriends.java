package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriends {

	public static void main(String[] args) throws Exception {
		// <input_file> <output_file>

		// args = new String[] { "./soc-LiveJournal1Adj.txt", "./output" };

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(CommonFriends.class);
		job.setMapperClass(CommonFriendsMapper.class);
		job.setReducerClass(CommonFriendsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);

		System.out.println(result ? 1 : 0);
	}

}
