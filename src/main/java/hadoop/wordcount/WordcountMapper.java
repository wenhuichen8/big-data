package hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏移量，Long-->hadoop 中有自己更精简序列化接口：LongWritable
 * VALUEIN：默认情况下，是mr框架所读到的一行文本的内容，String--》同上用 Text
 * KEYOUT 是用户定义逻辑处理完成之后输出数据中的key，自己定义（这里是String）--》同上用 Text
 * VALUEOUT 是用户定义逻辑处理完成之后输出数据中的value，自己定义（这里是Integer）--》IntWritable
 *
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/7
 * Time: 22:57
 * To change this template use File | Settings | File Templates.
 */
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    /**
     * map阶段的业务逻辑，maptask会对每一行输入的数据调用一次我们自定义的map方法
     * @param key   起始偏移量
     * @param value 读入的一行数据
     * @param context 将要写出去的信息
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将maptask传给我们的文本转成string
        String line = value.toString();
        String[] words = line.split(" ");
        //将单词输出为<单词，1>，后面的分发是按key进行分发的，所以这里是按单词进行分发，以便于后面单词会到相同的reduce中
        //不会马上发给reduce task，先收集
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
