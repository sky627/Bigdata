package hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonFriendsNameDateMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	private Map<String, String> map = new HashMap<String, String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path path = new Path(conf.get("userdata"));
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line = br.readLine();
		while (line != null) {
			String[] record = line.split(",");
			StringBuilder sb = new StringBuilder();
			sb.append(record[1]).append(" ").append(record[2]).append(",").append(record[9]);
			map.put(record[0], sb.toString());
			line = br.readLine();
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		if (line.length == 2) {
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			String[] friendList = line[1].split(",");
			for (String friend : friendList) {
				String[] record = map.get(friend).split(",");
				sb.append(record[0]).append(":").append(record[1]).append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("]");
			context.write(new Text(line[0]), new Text(sb.toString()));
		}
	}
}
