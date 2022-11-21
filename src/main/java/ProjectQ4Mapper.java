import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ProjectQ4Mapper extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().replaceAll("\"", "");
        String[] line_split = line.split(",");

        String symbol = line_split[0];
        String[] x = line_split[1].split(" ");
        String date = x[0];
        String time = x[1];
        int users = Integer.parseInt(line_split[2]);


        context.write(new CompositeKey(new Text(date), new Text(symbol)), new CompositeValue(new Text(time), new IntWritable(users)));

    }


}