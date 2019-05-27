
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
/**
 * @program: LAB3_MapReduceExample
 * @description:
 * @author: E1ixir_zzZ
 * @create: 2019-05-27 08:19
 **/
public class ChildAndParents {
    private static Configuration conf = new Configuration();
    private static String locpath = "hdfs://localhost:9000";

    private static void init() throws Exception {
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
    }

    public static class Map extends Mapper<Object,Text,Text,Text>{
        public void map(Object Key, Text value, Context context) throws IOException,InterruptedException{
            String line=value.toString();
            String[] words=line.split(" ");
            System.out.println(words.length);
            String parents=words[1];
            String child=words[0];
            System.out.println("*****************\n"+parents+"___"+child);


            String relationType="1";
            context.write(new Text(parents),new Text(relationType+"_"+child));
            relationType="2";
            context.write(new Text(child),new Text(relationType+"_"+parents));
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
            List<String> GrandParent=new ArrayList<String>(0);
            List<String> GrandKid=new ArrayList<String>(0);
            for(Text value: values){
                String line=value.toString();
                String type=line.split("_")[0];
                String name=line.split("_")[1];
                if("1".equals(type)){
                    GrandKid.add(name);
                }
                else{
                    GrandParent.add(name);
                }
            }

            for(String grandkid:GrandKid){
                for(String grandparent:GrandParent){
                    context.write(new Text(grandkid),new Text(grandparent));
                }
            }
        }
    }

    public static void main(String [] args)throws Exception{
        init();
        String InputDir = locpath + "/test/ChildParentInput";
        String OutputDir = locpath + "/test/ChildParentOutput";
        FileSystem hdfs=FileSystem.get(conf);
        if(hdfs.exists(new Path(OutputDir))){
            hdfs.delete(new Path(OutputDir),true);
        }
        Job job=Job.getInstance(conf,"ChildAndParent");
        job.setJarByClass(ChildAndParents.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path(OutputDir));
        FileInputFormat.addInputPath(job,new Path(InputDir));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
