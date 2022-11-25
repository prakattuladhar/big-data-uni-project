package ProjectQ5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ProjectQ5Job2Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

    boolean isDownBuy(float price, int user) {

        if (price < 0 && user > 0) {

            return true;
        } else {
            return false;
        }
    }

    boolean isUpSale(float price, int user) {

        if (price < 0 && user > 0) {

            return true;
        } else {
            return false;
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().replaceAll("\"", "");
        String[] line_split = line.split("\\s");

        String symbol = line_split[0];
        String[] date = line_split[1].split("-");
        String year = date[0];
        String month = date[1];
        float price = Float.valueOf(line_split[2]);
        int users = Integer.valueOf(line_split[3]);
        boolean downBuy = isUpSale(price, users);


//        if (downBuy) {
//            System.out.println("=============");
//            System.out.println(symbol + " " + year + "-" + month + " " + price + " " + users);
//            System.out.println(line_split[2]);
//            System.out.println(line_split[3]);
//            System.out.println("=============");
//        }


        context.write(new CompositeKey(new Text(symbol), new Text(year + "-" + month)), new Text(String.valueOf(downBuy)));


    }


}