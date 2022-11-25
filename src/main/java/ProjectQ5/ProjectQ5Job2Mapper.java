package ProjectQ5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormatSymbols;


public class ProjectQ5Job2Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

    boolean isDownBuy(float price, int user) {

        if (price < 0 && user > 0) {

            return true;
        } else {
            return false;
        }
    }

    boolean isUpSale(float price, int user) {

        if (price > 0 && user < 0) {

            return true;
        } else {
            return false;
        }
    }

    String getMonthForInt(int num) {
        String month = "wrong";
        DateFormatSymbols dfs = new DateFormatSymbols();
        String[] months = dfs.getMonths();
        if (num >= 0 && num <= 11) {
            month = months[num];
        }
        return month;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().replaceAll("\"", "");
        String[] line_split = line.split("\\s");

        String symbol = line_split[0];
        String[] date = line_split[1].split("-");
        String year = date[0];
        String month = getMonthForInt(Integer.valueOf(date[1])-1);
        float price = Float.valueOf(line_split[2]);
        int users = Integer.valueOf(line_split[3]);
        boolean downBuy = isDownBuy(price, users);
        boolean upSale = isUpSale(price, users);


        context.write(new CompositeKey(new Text(symbol), new Text(year + "-" + month)), new Text(String.valueOf(downBuy)));


    }


}