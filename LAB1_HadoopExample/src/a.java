/**
 * @program: kkk
 * @description:
 * @author: E1ixir_zzZ
 * @create: 2019-05-06 09:22
 **/

import jdk.internal.util.xml.impl.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;

import java.io.*;
import java.net.URL;
import java.util.Random;
import java.util.Scanner;

public class a {
    static FileSystem hdfs;
    static Configuration conf=new Configuration();


    static String locpath="hdfs://localhost:9000";

    static {
        try {
            conf.set("fs.defaultFS","hdfs://localhost:9000");
            conf.set("dfs.replication","1");
            hdfs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static boolean existFile(String fileName) throws  Exception{
        Path f=new Path(fileName);
        return hdfs.exists(f);
    }

        /*
        拷贝文件到hdfws
         */

        public static void copyFile(String localSrc,String dst,Configuration conf)throws Exception{
            File file=new File(localSrc);
            dst=locpath+dst;
            Path p=new Path(dst);
            FSDataOutputStream output=null;
            if(!existFile(dst)){
                InputStream in=new BufferedInputStream(new FileInputStream(file));
                OutputStream out= hdfs.create(p);
                IOUtils.copyBytes(in,out,4096,true);
                in.close();
                System.out.println("put the file"+localSrc+"to the hdfs"+dst);
            }
            else{
                System.out.println("file exists , would you want to  append write it or override it");
                System.out.print("please input 1 or 2");
                Scanner sc=new Scanner(System.in);
                int c=sc.nextInt();
                //追加
                if(c==1){
                    InputStream in=new BufferedInputStream(new FileInputStream(file));
                    output=hdfs.append(p);
                    IOUtils.copyBytes(in,output,4096,true);
                    in.close();
                    output.close();
                    System.out.println(" the append is finished");
                }
                // 重写
                else{
                    InputStream in=new BufferedInputStream(new FileInputStream(file));
                    try{hdfs.delete(p,true);}
                    catch (Exception e){
                        e.printStackTrace();
                    }
                    output=hdfs.create(p);
                    IOUtils.copyBytes(in,output,4096,true);
                    in.close();
                    output.close();
                    System.out.println("the overwrite is finished");
                }

            }

        }
    /*
     移到本地
     */
     public static void FileToLocal(String localsdst,String hdfs_src,Configuration conf)throws Exception{
         Path p= new Path(hdfs_src);
         File f=new File(localsdst);
         FSDataInputStream in=hdfs.open(new Path(hdfs_src));
         //不存在，则无需改名，直接复制
         if(!f.exists()) {
             OutputStream out=new BufferedOutputStream(new FileOutputStream(f));
             IOUtils.copyBytes(in,out,4096,true);
             in.close();
             out.close();
             System.out.println("get the file "+hdfs_src+" to the local "+localsdst);
         }
         //改名添加
         else{
             File file=f;
             String rs=null;
             while(file.exists()) {
                 String random=GenerateRandomString();
                 String tmp=localsdst.split("\\.")[0];
                 String tail=localsdst.split("\\.")[1];
                 rs = tmp+"_"+random+"."+tail;
                 file = new File(rs);
             }
             file.createNewFile();
             OutputStream out=new BufferedOutputStream(new FileOutputStream(f));
             IOUtils.copyBytes(in,out,4096,true);
             in.close();
             out.close();
             System.out.print("rename :");
             System.out.println("get the file "+hdfs_src+" to the local "+rs);
         }

     }

     private static String GenerateRandomString(){
         String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
         Random r=new Random();
         StringBuffer sb=new StringBuffer();
         for(int i=0;i<8;i++){
             int n=r.nextInt(62);
             sb.append(str.charAt(n));

         }
         return sb.toString();
     }


    public static void main (String[]args) throws Exception {
         a test=new a();
         test_put(test);
//        test_get(test);
    }

    private static void test_put(a test){
         String src="/Users/e1ixir/k.txt";
         String des="/k2_put.txt";
         try {
             a.copyFile(src, des, conf);
         }catch (Exception e){
             e.printStackTrace();
         }
    }
    private static void test_get(a test){
         String src="/test_get.txt";
         String des="/Users/e1ixir/test_get_out.txt";
         try{
             a.FileToLocal(des,src,conf);
         }catch (Exception e){
             e.printStackTrace();
         }
    }
}
