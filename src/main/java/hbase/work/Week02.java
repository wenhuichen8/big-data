package hbase.work;

import hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/13
 * Time: 22:28  xx f
 * java -classpath /usr/lib/hbase-current/lib/*.jar big-data.jar hbase.work.Week02
 * java -classpath .;/usr/lib/hbase-current/lib/*.jar hbase.work.Week02
 * To change this template use File | Settings | File Templates.
 */
public class Week02 {
    public static void main(String[] args) throws Exception {
        Connection conn = HBaseUtil.getConnection();//获取连接
        HBaseUtil.createNamespace(conn, "chenwenhui");//创建命名空间
        HBaseUtil.CreateTable(conn, "chenwenhui:student", "info", "score");//创建表
        //添加数据====================================================
        HTable table = (HTable) conn.getTable(TableName.valueOf("chenwenhui:student"));
        HBaseUtil.putData(table,"Tom","info","student_id","20210000000001");
        HBaseUtil.putData(table,"Tom","info","class","1");
        HBaseUtil.putData(table,"Tom","score","understanding","75");
        HBaseUtil.putData(table,"Tom","score","programming","82");

        HBaseUtil.putData(table,"Jerry","info","student_id","20210000000002");
        HBaseUtil.putData(table,"Jerry","info","class","2");
        HBaseUtil.putData(table,"Jerry","score","understanding","85");
        HBaseUtil.putData(table,"Jerry","score","programming","67");

        HBaseUtil.putData(table,"Jack","info","student_id","20210000000003");
        HBaseUtil.putData(table,"Jack","info","class","2");
        HBaseUtil.putData(table,"Jack","score","understanding","80");
        HBaseUtil.putData(table,"Jack","score","programming","80");

        HBaseUtil.putData(table,"Rose","info","student_id","20210000000004");
        HBaseUtil.putData(table,"Rose","info","class","2");
        HBaseUtil.putData(table,"Rose","score","understanding","60");
        HBaseUtil.putData(table,"Rose","score","programming","61");

        HBaseUtil.putData(table,"chenwenhui","info","student_id","G20220735030012");
        HBaseUtil.putData(table,"chenwenhui","info","class","9");
        HBaseUtil.putData(table,"chenwenhui","score","understanding","99");
        HBaseUtil.putData(table,"chenwenhui","score","programming","100");

        HBaseUtil.putData(table,"test","info","student_id","Test00000000001");
        HBaseUtil.putData(table,"test","info","class","8");
        HBaseUtil.putData(table,"test","score","understanding","88");
        HBaseUtil.putData(table,"test","score","programming","98");

        HBaseUtil.getTableByRowKey(conn, "chenwenhui:student", "chenwenhui");//查看一行数据
        HBaseUtil.scanTable(conn, "chenwenhui:student");//查看全部数据

        HBaseUtil.deleteByRowKey(conn, "chenwenhui:student", "test");//删除数据
        HBaseUtil.scanTable(conn, "chenwenhui:student");//查看全部数据

        HBaseUtil.deleteTable(conn, "chenwenhui:student");//删除表
        HBaseUtil.deleteNameSpace(conn, "chenwenhui");//删除NameSpace
        HBaseUtil.close(conn);
    }
}
