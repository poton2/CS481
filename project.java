import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException; 
import java.util.HashSet; 
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class project extends  Configured implements Tool{
    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new project(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception{
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Task 1");
        job.setJarByClass(project.class);
        job.setMapperClass(task1_mapper.class);
        job.setReducerClass(Task1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class task1_mapper extends Mapper<Object,Text,Text,Text>{

        public void map (Object key, org.w3c.dom.Text value, Context context)throws IOException, InterruptedException{
            String line = value.toString();
        if (!line.startsWith("ObjTimestamp")) { // Skip header line
            String[] fields = line.split(",");
            String objId = fields[1];
            String objData = line.substring(line.indexOf(",")+1);
            context.write(new Text(objId), new Text(objData));
            }
        }
    }

    public class Task1Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
    
            StringBuilder output = new StringBuilder();
    
            for (Text value : values) {
                output.append(value.toString());
                context.write(key,new Text(output.toString()));
            }
        }
    }
}
