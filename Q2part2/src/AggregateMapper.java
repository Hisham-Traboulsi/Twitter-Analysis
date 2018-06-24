import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class AggregateMapper extends Mapper<Object, Text, Text, IntWritable> {

private IntWritable numInstances = new IntWritable(1);
private Text hashtag = new Text();
private int hourOfTweet;
private final String regex = "#(\\w+)";
private final int MAX_LENGTH = 140;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

					String[] lines = value.toString().split(";");

					try
          {
              if(lines.length == 4)
              {
                  LocalDateTime timehour = LocalDateTime.ofEpochSecond(Long.parseLong(lines[0])/1000, 0, ZoneOffset.UTC);
                  hourOfTweet = timehour.getHour();

                   if(hourOfTweet == 1)
                   {
                        Pattern pattern = Pattern.compile(regex);
                        Matcher matcher = pattern.matcher(lines[2].toLowerCase());

                        while(matcher.find())
                        {
                            for(int i = 1; i<= matcher.groupCount(); i++)
                            {
                              hashtag.set(matcher.group(i));
                              context.write(hashtag, numInstances);
                            }
                        }
                    }
               }
					}
          catch(Exception ex)
          {
						System.out.println("Error: " + ex);
					}
      }

}
