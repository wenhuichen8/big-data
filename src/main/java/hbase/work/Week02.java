package hbase.work;

import hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/13
 * Time: 22:28
 * java -classpath /usr/lib/hbase-current/lib/*.jar big-data.jar hbase.work.Week02
 * java -classpath .;/usr/lib/hbase-current/lib/*.jar hbase.work.Week02
 * To change this template use File | Settings | File Templates.
 */
public class Week02 {
    public static void main(String[] args)throws Exception {
        Connection conn = HBaseUtil.getConnection();//获取连接
        HBaseUtil.createNamespace(conn,"chenwenhui");//创建命名空间
        HBaseUtil.CreateTable(conn,"chenwenhui:student","info","score");//创建表
        //添加数据====================================================
        HTable table = (HTable) conn.getTable(TableName.valueOf("chenwenhui:student"));

        Put put=new Put("Tom".getBytes());
        put.addColumn("info".getBytes(),"student_id".getBytes(),"20210000000001".getBytes());
        put.addColumn("info".getBytes(),"class".getBytes(),"1".getBytes());
        put.addColumn("score".getBytes(),"understanding".getBytes(),"75".getBytes());
        put.addColumn("score".getBytes(),"programming".getBytes(),"82".getBytes());
        table.put(put);

        Put put2=new Put("Jerry".getBytes());
        put2.addColumn("info".getBytes(),"student_id".getBytes(),"20210000000002".getBytes());
        put2.addColumn("info".getBytes(),"class".getBytes(),"2".getBytes());
        put2.addColumn("score".getBytes(),"understanding".getBytes(),"85".getBytes());
        put2.addColumn("score".getBytes(),"programming".getBytes(),"67".getBytes());
        table.put(put2);

        Put put3=new Put("Jack".getBytes());
        put3.addColumn("info".getBytes(),"student_id".getBytes(),"20210000000003".getBytes());
        put3.addColumn("info".getBytes(),"class".getBytes(),"2".getBytes());
        put3.addColumn("score".getBytes(),"understanding".getBytes(),"80".getBytes());
        put3.addColumn("score".getBytes(),"programming".getBytes(),"80".getBytes());
        table.put(put3);

        Put put4=new Put("Rose".getBytes());
        put4.addColumn("info".getBytes(),"student_id".getBytes(),"20210000000004".getBytes());
        put4.addColumn("info".getBytes(),"class".getBytes(),"2".getBytes());
        put4.addColumn("score".getBytes(),"understanding".getBytes(),"60".getBytes());
        put4.addColumn("score".getBytes(),"programming".getBytes(),"61".getBytes());
        table.put(put4);

        Put put5=new Put("chenwenhui".getBytes());
        put5.addColumn("info".getBytes(),"student_id".getBytes(),"G20220735030012".getBytes());
        put5.addColumn("info".getBytes(),"class".getBytes(),"8".getBytes());
        put5.addColumn("score".getBytes(),"understanding".getBytes(),"99".getBytes());
        put5.addColumn("score".getBytes(),"programming".getBytes(),"100".getBytes());
        table.put(put5);

        Put put6=new Put("test".getBytes());
        put6.addColumn("info".getBytes(),"student_id".getBytes(),"Test00000000001".getBytes());
        put6.addColumn("info".getBytes(),"class".getBytes(),"8".getBytes());
        put6.addColumn("score".getBytes(),"understanding".getBytes(),"99".getBytes());
        put6.addColumn("score".getBytes(),"programming".getBytes(),"100".getBytes());
        table.put(put6);

        HBaseUtil.getTableByRowKey(conn,"chenwenhui:student","chenwenhui");//查看一行数据
        HBaseUtil.scanTable(conn,"chenwenhui:student");//查看全部数据

        HBaseUtil.deleteByRowKey(conn,"chenwenhui:student","test");//删除数据
        HBaseUtil.scanTable(conn,"chenwenhui:student");//查看全部数据

       // HBaseUtil.deleteTable(conn,"chenwenhui:student");//删除表
       // HBaseUtil.deleteNameSpace(conn,"chenwenhui");//删除NameSpace
    }
}
