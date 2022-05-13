package hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/13
 * Time: 20:55
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUtil {
    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();        ;
        configuration.set("hbase.zookeeper.quorum", "emr-worker-1,emr-worker-2,emr-header-1");
        //configuration.set("hbase.zookeeper.quorum", "192.168.33.100")
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        //configuration.set("hbase.master", "192.168.33.100:60000");
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 创建表
     *
     * @param conn      连接
     * @param tabName   表名
     * @param colFamily 列族
     * @return 成功
     */
    public static boolean CreateTable(Connection conn, String tabName, String... colFamily) {
        try {
            Admin admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(tabName);
            // 建表
            if (admin.tableExists(tableName)) {
                System.out.println("Table already exists");
            } else {
                TableDescriptorBuilder tableDb = TableDescriptorBuilder.newBuilder(tableName);
                List<ColumnFamilyDescriptor> listColumnFamily = new ArrayList<>();
                for (String family : colFamily) {
                    ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build();
                    listColumnFamily.add(familyDescriptor);
                }
                tableDb.setColumnFamilies(listColumnFamily);
                TableDescriptor tableDescriptor = tableDb.build();
                admin.createTable(tableDescriptor);
                System.out.println("Table create successful");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 创建命名空间
     *
     * @param conn      连接
     * @param nameSpace 命名空间名字
     * @return
     */
    public static boolean createNamespace(Connection conn, String nameSpace) {
        try {
            //创建命名空间描述器
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            conn.getAdmin().createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 创建命名空间
     *
     * @param conn      连接
     * @param tableName 表名
     * @param rowKey    命名空间名字
     * @return
     */
    public static boolean getTableByRowKey(Connection conn, String tableName, String rowKey) {
        try {
            // 查看数据
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                Result result = conn.getTable(TableName.valueOf(tableName)).get(get);
                String colName;
                String value;
                String family;
                System.out.println("======rowKey:" + rowKey + "=========================");
                for (Cell cell : result.rawCells()) {
                    colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    family = Bytes.toString(CellUtil.cloneFamily(cell));
                    System.out.println("Data get success, family:" + family + ", colName: " + colName + ", value: " + value);
                }
                System.out.println("==================================================");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;

    }

    public static boolean scanTable(Connection conn, String tableName) {
        try {
            Scan scan = new Scan();
            Table table = conn.getTable(TableName.valueOf(tableName));//获得表对象
            ResultScanner rs = table.getScanner(scan);//得到扫描的结果集
            for (Result result : rs) {//得到单元格集合
                List<Cell> cs = result.listCells();
                for (Cell cell : cs) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));//取行健
                    long timestamp = cell.getTimestamp();//取到时间戳
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));//取到族列
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));//取到修饰名
                    String value = Bytes.toString(CellUtil.cloneValue(cell));//取到值
                    System.out.println(" ===> rowKey : " + rowKey + ", timestamp : " +
                            timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean deleteByRowKey(Connection conn, String tableName, String rowKey) {
        try {
            // 删除数据
            Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
            conn.getTable(TableName.valueOf(tableName)).delete(delete);
            System.out.println("Delete Success");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean deleteTable(Connection conn, String tabName) {
        try {
            Admin admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(tabName);
            // 删除表
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("Table Delete Successful");
            } else {
                System.out.println("Table does not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean deleteNameSpace(Connection conn, String nameSpance) {
        try {
            Admin admin = conn.getAdmin();
            admin.deleteNamespace(nameSpance);
            System.out.println("NameSpance:" + nameSpance + " Delete Successful");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
