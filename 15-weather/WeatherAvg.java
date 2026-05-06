import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAvg {

 public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

   String[] parts = value.toString().split(",");

   if (parts.length == 4) {

    
    context.write(new Text("Temperature"), new FloatWritable(Float.parseFloat(parts[1])));
    
    context.write(new Text("DewPoint"), new FloatWritable(Float.parseFloat(parts[2])));
    context.write(new Text("WindSpeed"), new FloatWritable(Float.parseFloat(parts[3])));
   }
  }
 }

 public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

  public void reduce(Text key, Iterable<FloatWritable> values, Context context)
          throws IOException, InterruptedException {

   float sum = 0;
   int count = 0;

   for (FloatWritable val : values) {
    sum += val.get();
    count++;
   }

   context.write(key, new FloatWritable(sum / count));
  }
 }

 public static void main(String[] args) throws Exception {

  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "Weather Average");

  job.setJarByClass(WeatherAvg.class);
  job.setMapperClass(Map.class);
  job.setReducerClass(Reduce.class);

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(FloatWritable.class);

  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}