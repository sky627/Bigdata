package hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserMaxAgeFriendReducer extends Reducer<Text, Text, Text, IntWritable> {
	private Map<String, String> map = new HashMap<String, String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(conf.get("userdata"));
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line = br.readLine();
		while (line != null) {
			String[] record = line.split(",");
			map.put(record[0], record[9]);
			line = br.readLine();
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String minDate = "99/99/9999";
		for (Text value : values) {
			String curDate = map.get(value.toString());
			if (compare(curDate, minDate)) {
				minDate = curDate;
			}
		}
		int age = 2020 - Integer.parseInt(minDate.split("/")[2]);
		context.write(key, new IntWritable(age));
	}

	private boolean compare(String date1, String date2) {
		String[] d1 = date1.split("/");
		String[] d2 = date2.split("/");
		if (d1[2].compareTo(d2[2]) < 0) {
			return true;
		} else if (d1[2].compareTo(d2[2]) > 0) {
			return false;
		} else {
			if (d1[0].compareTo(d2[0]) < 0) {
				return true;
			} else if (d1[0].compareTo(d2[0]) > 0) {
				return false;
			} else {
				if (d1[1].compareTo(d2[1]) <= 0) {
					return true;
				} else {
					return false;
				}
			}
		}
	}
}