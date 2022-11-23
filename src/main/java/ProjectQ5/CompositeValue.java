package ProjectQ5;//import org.apache.curator.shaded.com.google.common.collect.ComparisonChain;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CompositeValue implements WritableComparable<CompositeValue> {

    public Text identifier;
    public FloatWritable price;
    public IntWritable users;

    public CompositeValue() {
        this.identifier = new Text();
        this.price = new FloatWritable();
        this.users = new IntWritable();
    }

    public CompositeValue(Text identifier, FloatWritable price, IntWritable users) {
        this.identifier = identifier;
        this.price = price;
        this.users = users;
    }


    @Override
    public int compareTo(CompositeValue o) {
        return ComparisonChain.start().compare(this.price.toString(), o.price.toString())
                .compare(this.users, o.users)
                .compare(this.identifier, o.identifier)
                .result();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        identifier.write(dataOutput);
        price.write(dataOutput);
        users.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        identifier.readFields(dataInput);
        price.readFields(dataInput);
        users.readFields(dataInput);
    }


    @Override
    public int hashCode() {
        return Objects.hash(price, users);
    }

    @Override
    public String toString() {
        return "(" + this.identifier + "," + this.price + "," + this.users + ")";

    }
}
