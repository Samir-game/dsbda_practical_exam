import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text logLevel = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Split log line
        String[] words = line.split(" ");

        // Assuming log level is 3rd word
        if (words.length > 2) {
            logLevel.set(words[2]);
            context.write(logLevel, one);
        }
    }
}