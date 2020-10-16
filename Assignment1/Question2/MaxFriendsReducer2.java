package hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxFriendsReducer2 extends Reducer<IntWritable, Text, Text, Text> {
	private int max = 0;

	private List<String> res = new LinkedList<String>();

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] line = value.toString().split("\t");
			int commonFriendsNum = line.length == 1 ? 0 : line[1].split(",").length;
			if (commonFriendsNum >= max) {
				if (commonFriendsNum > max) {
					max = commonFriendsNum;
					res.clear();
				}
				res.add(value.toString());
			}
		}
		for (String s : res) {
			String[] tmp = s.split("\t");
			context.write(new Text(tmp[0]), new Text(tmp[1]));
		}
	}
}
