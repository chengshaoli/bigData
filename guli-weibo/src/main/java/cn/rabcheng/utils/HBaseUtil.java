package cn.rabcheng.utils;

import cn.rabcheng.constants.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-04-01 9:54
 */
public class HBaseUtil {
    /**
     * 创建命名空间
     * @param namespace 命名空间
     * @throws IOException io
     */
    public static void creaeNameSpace(String namespace) throws IOException {

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Admin admin = conn.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

        admin.createNamespace(namespaceDescriptor);

        admin.close();
        conn.close();
    }

    /**
     * 判断表是否存在
     * @param table 表明
     * @return 是否存在 true表示存在 false表示不存在
     * @throws IOException io
     */
    private boolean isTableExists(String table) throws IOException {

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Admin admin = conn.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf(table));

        admin.close();
        conn.close();

        return exists;
    }

    /**
     *  创建表
     * @param tableName 表名
     * @param versions 版本数量
     * @param cfs 列族
     * @throws IOException io
     */
    public static void createTable(String tableName,int versions,String ... cfs) throws IOException {

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Admin admin = conn.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //添加列族和版本数量
        for (String cf : cfs) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            hColumnDescriptor.setMaxVersions(versions);

            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);

        admin.close();
        conn.close();
    }

}
