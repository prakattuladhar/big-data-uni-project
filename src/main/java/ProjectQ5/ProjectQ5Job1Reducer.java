package ProjectQ5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProjectQ5Job1Reducer extends Reducer<CompositeKey, CompositeValue, Text, Text> {


    public void reduce(CompositeKey key, Iterable<CompositeValue> values, Context context)
            throws IOException, InterruptedException {

        String price = "";
        String users = "";
        for (CompositeValue val : values) {
            System.out.println(val.toString());
            if (val.identifier.toString().equals("price")) {
                price = String.valueOf(val.price.get());
            } else if (val.identifier.toString().equals("users")) {
                users = String.valueOf(val.users.get());

            }
            if (users.equals("")) {
                users = "0";
            }
        }

        if (!users.equals("") && !price.equals(""))
            context.write(new Text(key.symbol + " " + key.date), new Text(price + " " + users));
    }

}
