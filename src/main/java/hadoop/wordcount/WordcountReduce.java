package hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN，VALUEIN 对应Mapper输出的KEYOUT、VALUEOUT数据类型
 * KEYOUT,VALUEOUT 是自己定义reduce逻辑处理的输出数据类型：
 * 这里为：
 * KEYOUT：单词--》Text
 * VALUEOUT：总次数---》IntWritable
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/7
 * Time: 23:40
 * To change this template use File | Settings | File Templates.
 */
public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 调用一次就得到一个单词的总数
     * @param key     是一组相同单词kv对的key  <hello,1><hello,2><hello,3><hello,4><hello,5> 这里是hello
     * @param values  是一组相同单词kv对的value，这里是1,2,3,4,5
     * @param context 将要写出去的信息
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count=count+value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
