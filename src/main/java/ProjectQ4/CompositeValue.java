package ProjectQ4;//import org.apache.curator.shaded.com.google.common.collect.ComparisonChain;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CompositeValue implements WritableComparable<CompositeValue> {

    public Text time;
    public IntWritable users;

    public CompositeValue() {
        this.time = new Text();
        this.users = new IntWritable();
    }

    public CompositeValue(Text time, IntWritable users) {
        this.time = time;
        this.users = users;
    }


    @Override
    public int compareTo(CompositeValue o) {
        return ComparisonChain.start().compare(this.time.toString(), o.time.toString())
                .compare(this.users, o.users)
                .result();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        time.write(dataOutput);
        users.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        time.readFields(dataInput);
        users.readFields(dataInput);
    }


    @Override
    public int hashCode() {
        return Objects.hash(time, users);
    }

    @Override
    public String toString() {
        return "(" + this.time + "," + this.users + ")";

    }
}
