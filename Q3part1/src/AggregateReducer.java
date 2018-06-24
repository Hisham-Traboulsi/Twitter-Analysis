import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

       //1: Compute the aggregate summarisation (average)

      int sum = 0;
      for(IntWritable value: values){

          sum += value.get();



      }

      result.set(sum);



      //2: emit the final result (feature, result)

      context.write(key, result);
    }
}
