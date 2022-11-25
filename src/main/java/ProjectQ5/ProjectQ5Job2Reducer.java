package ProjectQ5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProjectQ5Job2Reducer extends Reducer<CompositeKey, Text, Text, Text> {


    public void reduce(CompositeKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        for (Text val : values) {
            if (val.toString().equals("true")) {
                count++;
            }

        }
     
        context.write(new Text(key.toString()), new Text(String.valueOf(count)));
    }

}
