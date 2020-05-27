package cn.rabcheng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @auther cheng
 * @create 2020-03-31 8:39
 */
public class HbaseApi {

    private static Connection conn;
    private static Admin admin;

    static {

        try {

            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop1");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");

            conn = ConnectionFactory.createConnection(configuration);
            admin = conn.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //判断表是否存在
    public boolean isTableExists(String table) throws IOException {

        boolean exists = false;
        exists = admin.tableExists(TableName.valueOf(table));

        if (exists) {
            System.out.println(table + "表存在");
        } else {
            System.out.println(table + "不存在");
        }
        return exists;

    }

    //创建表
    public void createTable(String table, String... cfs) throws IOException {

        boolean exists = isTableExists(table);

        if (!exists) {

            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(table));


            if (cfs.length != 0) {
                for (String cf : cfs) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
            }

            admin.createTable(hTableDescriptor);

            System.out.println("表" + table + "已经创建成功！");

        } else {
            System.out.println(table + "表已经存在了");
        }


    }

    //删除表
    public void dropTable(String table) throws IOException {

        if (isTableExists(table)) {

            admin.disableTable(TableName.valueOf(table));

            admin.deleteTable(TableName.valueOf(table));

            System.out.println("表" + table + "删除成功！");

        } else {
            System.out.println("表" + table + "不存在");
        }

    }

    //创建命名空间
    public void createNameSpace(String namespace) throws IOException {

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();


        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println("namespace" + namespace + "已经存在！");
        }


    }

    //删除命名空间
    public void deleteNameSpace(String namespace) throws IOException {
        admin.deleteNamespace(namespace);
    }

    //向表中添加数据
    public void putData(String table, String row, String columnFamily, String column, String value) throws IOException {

        //获取表对象
        Table connTable = conn.getTable(TableName.valueOf(table));

        //创建put对象
        Put put = new Put(Bytes.toBytes(row));

        //给put对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        //插入数据
        connTable.put(put);

        //关闭资源
        connTable.close();

    }

    //获取数据
    public void getData(String table, String row) throws IOException {

        Table connTable = conn.getTable(TableName.valueOf(table));

        Get get = new Get(Bytes.toBytes(row));
        //指定列、列族、版本数
//        get.addFamily()
//        get.addColumn()
//        get.setMaxVersions(3)
        Result result = connTable.get(get);

        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(cell.getRow()));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        connTable.close();

    }

    //Scan表
    public void scanTable(String table) throws IOException {
        Table connTable = conn.getTable(TableName.valueOf(table));

        Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1003"));//左闭右开
//        scan.addColumn()
        ResultScanner scanner = connTable.getScanner(scan);

        for (Result result : scanner) {

            for (Cell cell : result.rawCells()) {

                System.out.println(Bytes.toString(cell.getRow()));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }

        connTable.close();
    }

    //删除表
    public void deleteData(String table,String row,String columnF,String column) throws IOException {

        Table connTable = conn.getTable(TableName.valueOf(table));

        Delete delete = new Delete(Bytes.toBytes(row));

//        delete.addColumn(Bytes.toBytes(columnF),Bytes.toBytes(column),152365988745L);//删除一个版本,此操作生产环境慎用！比如说删除一个版本后，下次查看的时候下一个版本会被查出，不符合逻辑
        delete.addColumns(Bytes.toBytes(columnF),Bytes.toBytes(column));//删除所有的版本

        connTable.delete(delete);

        connTable.close();

    }

    public static void main(String[] args) {

        HbaseApi hbaseApi = new HbaseApi();


        try {
            //判断表是否存在
//            hbaseApi.isTableExists("student");

            //创建表
//            hbaseApi.createTable("stu:stu","info","instrist");

            //删除表
//            hbaseApi.dropTable("stu");

            //创建命名空间
//            hbaseApi.createNameSpace("stu");

            //添加数据
//            hbaseApi.putData("student","1001","info","name","zhangsan");

            //获取数据
//            hbaseApi.getData("student","1001");

            //scan表
//            hbaseApi.scanTable("student");

            //删除数据
            hbaseApi.deleteData("student","1001","info","name");
        } catch (IOException e) {
            e.printStackTrace();
        }


        //关闭资源
        hbaseApi.close();

    }

}
