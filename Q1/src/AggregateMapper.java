import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AggregateMapper extends Mapper<Object, Text, Text, IntWritable> {

	private IntWritable value = new IntWritable(1);
	private final static int MAX_LENGTH = 140;
	private static int startRange = 0, endRange = 0;
	private static Text ranges = new Text();

	 public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

	 String [] lines = input.toString().split(";");

		 if (lines.length == 4 && lines[2].length() <= MAX_LENGTH)
		 {
			 		int tweetLength = lines[2].length();

				 	if (tweetLength % 5 == 0)
				 	{
						 	startRange = tweetLength - 4;
						 	endRange = startRange + 4;
				 	}
				 	else
				 	{
						 	startRange = tweetLength - (tweetLength % 5) + 1;
						 	endRange = startRange + 4;
				 	}
				 	ranges.set(startRange + "-" + endRange);
				 	context.write(ranges, value);
			}
	 }
}
