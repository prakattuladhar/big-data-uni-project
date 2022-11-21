//import org.apache.curator.shaded.com.google.common.collect.ComparisonChain;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CompositeKey implements WritableComparable<CompositeKey> {

    public Text symbol;
    public Text date;

    public CompositeKey() {
        this.symbol = new Text();
        this.date = new Text();
    }

    public CompositeKey(Text symbol, Text date) {
        this.symbol = symbol;
        this.date = date;
    }


    @Override
    public int compareTo(CompositeKey o) {
        return ComparisonChain.start().compare(this.symbol.toString(), o.symbol.toString())
                .compare(this.date.toString(), o.date.toString())
                .result();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        symbol.write(dataOutput);
        date.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        symbol.readFields(dataInput);
        date.readFields(dataInput);
    }


    @Override
    public int hashCode() {
        return Objects.hash(symbol, date);
    }

    @Override
    public String toString() {
        return "(" + this.symbol + "," + this.date + ")";

    }
}
