package hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包，最后提交给yarn
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/8
 * Time: 0:01
 * To change this template use File | Settings | File Templates.
 */
public class WordMain {
    public static void main(String[] args) {
        Configuration config = new Configuration();
//        config.set("mapreduce.framework.name","yarn");
//        config.set("yarn.resoucemanger.hostname","node01");
        try {
            Job job = Job.getInstance(config);

            //指定这个job用的那个map task，因为一个工程里有很多的map task
            job.setMapperClass(WordcountMapper.class);
            //指定这个job用的那个reduce task，因为一个工程里有很多的reduce task
            job.setReducerClass(WordcountReduce.class);

            //指定Map输出数据的key/value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //指定最终输出的数据的key/value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //指定job的输入原始文件所在地址
            FileInputFormat.setInputPaths(job,"/wordcount/input");
            //指定job的输出结果存放地址
            FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));

            //指定本程序的jar包所在的本地路径,yarn需要知道jar的位置
            job.setJarByClass(WordMain.class);
            //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
//            job.submit(); //提交后不管了，后端运行
            boolean reslut = job.waitForCompletion(true);//提交后等待反馈结果
            System.exit(reslut?0:1);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
