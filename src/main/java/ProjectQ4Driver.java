import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProjectQ4Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: program <input dir> <output dir>\n");
            System.exit(-1);
        }
        Job job = new Job();

        job.setJarByClass(ProjectQ4Driver.class);
        job.setJobName("Question2 Job1");//to accept the hdfs input and output dir at run time

        String baseInputDir=args[0];
        job.getConfiguration().set("BaseInputPath",baseInputDir);
        FileInputFormat.addInputPath(job, new Path(baseInputDir+"/*")); // /* will recursively iterate through the directory giving access to nested files
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//setting the class names

        /* Mapper*/
        job.setMapperClass(ProjectQ4Mapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(CompositeValue.class);//setting the output data type classes
        job.setNumReduceTasks(1);
////        /* Reducer */
        job.setReducerClass(ProjectQ4Reducer.class);
////
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
