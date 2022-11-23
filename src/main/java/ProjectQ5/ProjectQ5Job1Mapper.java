package ProjectQ5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ProjectQ5Job1Mapper extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().replaceAll("\"", "");
        String[] line_split = line.split("\\s");
        if (line_split.length == 3) {
            String symbol = line_split[0];
            String date = line_split[1];
            int users = Integer.parseInt(line_split[2]);
            context.write(new CompositeKey(new Text(date), new Text(symbol)), new CompositeValue(new Text("users"), new FloatWritable(), new IntWritable(users)));
        } else if (line_split.length == 8) {
            String symbol = line_split[1];
            String date = line_split[0];
            float diff = Float.parseFloat(line_split[2]) - Float.parseFloat(line_split[5]);
            context.write(new CompositeKey(new Text(date), new Text(symbol)), new CompositeValue(new Text("price"), new FloatWritable(diff), new IntWritable()));
        }


    }


}