import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;




public class AggregateMapper extends Mapper<Object, Text, Text, IntWritable> {

		private int lengthOfTweet;
		private IntWritable data = new IntWritable();
    private IntWritable instances = new IntWritable(1);
		private Text name = new Text();

		private ArrayList<String> athletesNames;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    		String []lines = value.toString().split(";");

				if(lines.length == 4 && lines[2].length()<=140)
				{
						for(int i = 0; i<athletesNames.size(); i++)
						{
								String athleteName = athletesNames.get(i);
								if(lines[2].contains(athleteName))
								{
										name.set(athleteName);
										context.write(name, instances);
								}
						}
				}

    }

		protected void setup(Context context) throws IOException, InterruptedException{

				this.athletesNames = new ArrayList<>();

				URI fileURI = context.getCacheFiles()[0];

				FileSystem fs = FileSystem.get(context.getConfiguration());
				FSDataInputStream in  = fs.open(new Path(fileURI));

				BufferedReader br = new BufferedReader(new InputStreamReader(in));

				String line =br.readLine();

				try{
					while((line = br.readLine()) != null){
						String[] fields = line.toString().split(",");
						if(fields.length == 11){
								this.athletesNames.add(fields[1]);
						}

					}
					br.close();
				}catch (IOException e1){
					System.out.println("Error: " + e1);
				}

				super.setup(context);
		}
}
