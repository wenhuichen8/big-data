package hadoop.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/9
 * Time: 18:07
 * To change this template use File | Settings | File Templates.
 */
public class FlowCount {

    public static void main(String[] args) throws Exception {
        String defInput = "/Flowcount/input/";
        String defOutput = "/Flowcount/output";
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        /*job.setJar("/home/hadoop/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        //指定本业务job要使用的mapper/Reducer 业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出的数据的KV数据，默认Maptask和ReduTask都使用这里的设置，所以当Maptask和ReduTask输出相同时，只需这里设置即可。
        // 当Maptask和ReduTask输出不相同时，使用MapOutputKeyClass和MapOutputValueClass设置Maptask的输出。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        Path inputPath = new Path(defInput);
        Path outputPath = new Path(defOutput);
        if (args[0] != null) {
            inputPath = new Path(args[0]);
        }
        if (args[1] != null) {
            outputPath = new Path(args[1]);
        }
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, inputPath);
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, outputPath);

        //同时指定相应“分区”数量的reducetask，默认ReduTasks为1,可以设置为0,，设置为0时表示没有ReduTasks
        job.setNumReduceTasks(1);

        //将job配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        //将任务提交给yarn，提交yarn后，等待yarn返回运行结果
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }


    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        /**
         * map 阶段的业务逻辑就是写在自定义的map（）方法中
         * maptask每读取一行数据，mr框架会调用一次这方法     *
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行内容转成string
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号
            String phoneNbr = fields[1];
            //取出上行流量下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long dFlow = Long.parseLong(fields[fields.length - 2]);

            context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        /**
         * @param key
         * @param values  注意：需指定使用自定义类：FlowBean
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlow = 0;
            long downFlow = 0;

            for (FlowBean bean : values) {
                upFlow = upFlow + bean.getUpFlow();
                downFlow += bean.getDownFlow();
            }
            context.write(key, new FlowBean(upFlow, downFlow));
        }
    }
}
