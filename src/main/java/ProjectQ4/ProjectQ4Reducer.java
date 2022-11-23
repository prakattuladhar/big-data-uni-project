package ProjectQ4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProjectQ4Reducer extends Reducer<CompositeKey, CompositeValue, Text, Text> {

    private int[] convert(String time) {
        String[] time_s = time.split(":");
        int[] tmp = {Integer.parseInt(time_s[0]), Integer.parseInt(time_s[1]), Integer.parseInt(time_s[2])};
        return tmp;
    }

    private boolean isSmaller(String left, String right) {
        int[] left_converted = convert(left);
        int[] right_converted = convert(right);

        for (int i = 0; i < 3; i++) {
            if (left_converted[i] < right_converted[i]) {
                return true;
            } else if (left_converted[i] > right_converted[i]) {
                return false;
            } else {
                // Do nothing. Its equal
            }
        }
        return false;
    }

    public void reduce(CompositeKey key, Iterable<CompositeValue> values, Context context)
            throws IOException, InterruptedException {

        int min_user = 0, max_user = 0;
        String min_time = null, max_time = null;

        for (CompositeValue val : values) {
            if (min_time == null && max_time == null) {
                min_time = val.time.toString();
                max_time = val.time.toString();
                min_user = val.users.get();
                max_user = val.users.get();
                continue;
            } else {
                if (isSmaller(val.time.toString(), min_time)) {
                    min_time = val.time.toString();
                    min_user = val.users.get();

                } else {
                    max_time = val.time.toString();
                    max_user = val.users.get();
                }
            }
        }
        int diff = max_user - min_user;
        context.write(new Text(key.symbol + " " + key.date), new Text(String.valueOf(diff)));
    }

}
