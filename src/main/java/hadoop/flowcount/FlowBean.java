package hadoop.flowcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/9
 * Time: 18:04
 * To change this template use File | Settings | File Templates.
 */
public class FlowBean implements WritableComparable {
    private long upFlow; //上传流量数
    private long downFlow;//下载流量数
    private long sumFlow; //总流量数

    //反序列化时，需要反射调用空参构造函数，所以要显示定义一个
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow = upFlow + downFlow;
    }


    //序列化（将个对象属性调用内部方法写出去就可了）
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化（调用内部方法从序列化流中读取信息，然后赋值给对象即可)
    //注意：反序列化的顺序跟序列化的顺序完全一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    //自定义倒序比较规则
    @Override
    public int compareTo(Object o) {
        return sumFlow > ((FlowBean) o).getSumFlow() ? -1 : 1;
    }

    @Override
    public String toString() {
       // return "FlowBean{" + "upFlow=" + upFlow + ", downFlow=" + downFlow + ", sumFlow=" + sumFlow + '}';
        return  upFlow + " " + downFlow + " " + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
