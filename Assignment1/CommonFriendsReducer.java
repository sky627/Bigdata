package hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		Set<String> set = new HashSet<String>();
		boolean hasCommonFriends = false;
		for (Text value : values) {
			String[] friends = value.toString().split(",");
			for (String friend : friends) {
				if (set.contains(friend)) {
					sb.append(friend).append(",");
					hasCommonFriends = true;
				}
				set.add(friend);
			}
		}
		if (hasCommonFriends) {
			sb.deleteCharAt(sb.length() - 1);
		}
		context.write(key, new Text(sb.toString()));
	}
}
