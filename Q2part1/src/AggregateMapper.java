import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;


public class AggregateMapper extends Mapper<Object, Text, Text, IntWritable> {

private IntWritable instances = new IntWritable(1);
private String hourOfTweet;
private Text hour = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
					String[] lines = value.toString().split(";");

          try
          {
              if(lines.length == 4)
              {

                LocalDateTime timehour = LocalDateTime.ofEpochSecond(Long.parseLong(lines[0])/1000, 0, ZoneOffset.UTC);
                hourOfTweet = Integer.toString(timehour.getHour());
                hour.set(hourOfTweet);
                context.write(hour, instances);
              }
          }
          catch(Exception ex)
          {
            System.out.println("Error: "+ex);
          }
	  }

}
