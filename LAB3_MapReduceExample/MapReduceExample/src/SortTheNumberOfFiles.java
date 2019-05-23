/**
 * @program: LAB3_MapReduceExample
 * @description:将不同文件下的数字进行排序输出到同一文件下
 * @author: E1ixir_zzZ
 * @create: 2019-05-23 11:07
 **/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortTheNumberOfFiles {
    private static Configuration conf = new Configuration();
    private static String locpath = "hdfs://localhost:9000";

    private static void init() throws Exception {
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
    }

    public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
        IntWritable keyInfo=new IntWritable();
        IntWritable valueInfo=new IntWritable(1);

        public void map(Object key, Text value,Context context)throws IOException,InterruptedException {
            String line=value.toString();
            FileSplit split=(FileSplit)context.getInputSplit();
            String filename=split.getPath().getName();
//            System.out.println("kkkk:   "+line+" "+filename);
            if(!(line==null || "".equals(line))){
//                System.out.println(line+" "+filename);
                keyInfo.set(Integer.parseInt(line));
                context.write(keyInfo,valueInfo);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        public IntWritable linenum= new IntWritable(1);
        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
            for(IntWritable value:values){
                IntWritable keyInfo=linenum;
                context.write(keyInfo,key);
                linenum=new IntWritable(linenum.get()+1);
            }
        }
    }




    public static void main(String [] args)throws  Exception{
        String InputDir=locpath+"/test/SortInput";
        String OutputDir=locpath+"/test/SortOutput";

        Job job=Job.getInstance(conf,"Sort");

        job.setJarByClass(SortTheNumberOfFiles.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Reduce.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(InputDir));
        FileOutputFormat.setOutputPath(job,new Path(OutputDir));
        System.exit(job.waitForCompletion(true)?0:1);

    }


}
