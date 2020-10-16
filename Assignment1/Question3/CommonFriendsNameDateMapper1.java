package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonFriendsNameDateMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String userA = conf.get("userA");
		String userB = conf.get("userB");
		String[] line = value.toString().split("\t");
		if (line.length == 2 && (line[0].equals(userA) || line[0].equals(userB))) {
			String[] friends = line[1].split(",");
			StringBuilder sb = new StringBuilder();
			for (String friend : friends) {
				if (friend.equals(userA) || friend.equals(userB)) {
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
}
