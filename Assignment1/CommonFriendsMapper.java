package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		String userA = line[0];
		if (line.length == 2) {
			String[] friendList = line[1].split(",");
			for (String userB : friendList) {
				StringBuilder sb = new StringBuilder();
				if (userA.compareTo(userB) < 0) {
					sb.append(userA).append(",").append(userB);
				} else {
					sb.append(userB).append(",").append(userA);
				}
				context.write(new Text(sb.toString()), new Text(line[1]));
			}
		}
	}
}
