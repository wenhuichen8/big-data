package hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by IntelliJ IDEA.
 * User: chenwenhui
 * Date: 2022/5/13
 * Time: 20:55
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUtil {
    private static final Logger log = getLogger(HBaseUtil.class);

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        ;
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
                log.info("table:{} already exists", tabName);
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
                log.info("table:{} create successful", tabName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("创建表失败：", e);
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
            log.info("NameSpace:{} create successful", nameSpace);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("创建命名空间：", e);
            return false;
        }
        return true;
    }

    /**
     * 表格添加数据
     * @param table
     * @param rowKey
     * @param colFamily
     * @param colKey
     * @param colvalue
     * @return
     */
    public static boolean putData(HTable table,String rowKey,String colFamily,String colKey,String colvalue) {
        try {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(colFamily.getBytes(), colKey.getBytes(), colvalue.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("表格添加数据失败：", e);
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
                log.info("======================rowkey:{}==================", rowKey);
                for (Cell cell : result.rawCells()) {
                    colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    family = Bytes.toString(CellUtil.cloneFamily(cell));
                    log.info("Data get success, family:{}, colName:{}, value:{}", family, colName, value);
                }
                log.info("=============================================");
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("查询数据失败：", e);
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
                    log.info(" ===> rowKey  family:{}, rowKey:{}, qualifier:{}, timestamp:{}, value:{}", family, rowKey, qualifier, timestamp, value);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("scanTable失败：", e);
            return false;
        }
        return true;
    }

    public static boolean deleteByRowKey(Connection conn, String tableName, String rowKey) {
        try {
            // 删除数据
            Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
            conn.getTable(TableName.valueOf(tableName)).delete(delete);
            log.info("Table：{} Delete rowkey：{} Success",tableName,rowKey);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("deleteByRowKey失败：", e);
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
                log.info("Table:{} Delete Successful",tableName);
            } else {
                log.info("Table:{} does not exist!",tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("deleteTable：", e);
            return false;
        }
        return true;
    }

    public static boolean deleteNameSpace(Connection conn, String nameSpance) {
        try {
            Admin admin = conn.getAdmin();
            admin.deleteNamespace(nameSpance);
            log.info("NameSpance:{} Delete Successful",nameSpance);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("deleteNameSpace 失败：", e);
            return false;
        }
        return true;
    }

    public static HTable addValTableDescriptor(Connection conn, String tableName) {
        HTable table = null;
        try {
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }


    public static boolean close(Connection conn) {
        try {
            if (conn != null) {
                if (conn.getAdmin() != null) {
                    conn.getAdmin().close();
                }
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
