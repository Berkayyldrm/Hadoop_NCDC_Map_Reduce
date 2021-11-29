import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanDriver 
{
           
            public static void main (String[] args) throws Exception
            {
                        if (args.length != 2)
                        {
                               System.err.println("Please Enter the input and output parameters");
                               System.exit(-1);
                        }
                       
                      Configuration conf = new Configuration();
                      Job job = Job.getInstance(conf, "mean temp");
                      job.setJarByClass(MeanDriver.class);
                      job.setJobName("Mean temperature");
                       
                      FileInputFormat.addInputPath(job,new Path(args[0]));
                      FileOutputFormat.setOutputPath(job,new Path (args[1]));
                      
                      
                      job.setMapperClass(MeanTemperatureMapper.class);
                      job.setReducerClass(MeanTemperatureReducer.class);
                       
                      job.setOutputKeyClass(Text.class);
                      job.setOutputValueClass(DoubleWritable.class);
                       
                      System.exit(job.waitForCompletion(true)?0:1);                                             
            }
}