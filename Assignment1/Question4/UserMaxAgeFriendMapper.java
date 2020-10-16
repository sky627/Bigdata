package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMaxAgeFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		String userA = line[0];
		if (line.length == 2) {
			String[] friendList = line[1].split(",");
			for (String userB : friendList) {
				context.write(new Text(userA), new Text(userB));
			}
		}
	}
}
