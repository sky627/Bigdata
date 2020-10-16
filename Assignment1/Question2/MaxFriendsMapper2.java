package hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxFriendsMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	private int max = 0;

	private List<String> list = new LinkedList<String>();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		int commonFriendsNum = line.length == 1 ? 0 : line[1].split(",").length;
		if (commonFriendsNum >= max) {
			if (commonFriendsNum > max) {
				max = commonFriendsNum;
				list.clear();
			}
			list.add(value.toString());
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String s : list) {
			context.write(new IntWritable(1), new Text(s));
		}
	}
}
