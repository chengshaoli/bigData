package cn.rabcheng.dao;

import cn.rabcheng.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1.发布微博
 * 2.关注用户
 * 3.取关用户
 * 4.获取用户微博详情
 * 5.获取用户初始化页面
 *
 * @auther cheng
 * @create 2020-04-01 10:10
 */
public class HBaseDao {

    //发布微博
    public static void publishWeiBo(String uid, String content) throws IOException {

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作微博内容表
        Table connTable = conn.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        String row = uid + "_" + (9999999999999L - System.currentTimeMillis());

        Put contput = new Put(Bytes.toBytes(row));

        contput.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        connTable.put(contput);

        //第二部分：操作微博收件箱表
        //从relation表中拿出uid的fans，将刚发布的内容加到每个fan收件箱

        Table relaTable = conn.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        Get relaGet = new Get(Bytes.toBytes(uid));
        relaGet.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));//获取指定的列族

        Result result = relaTable.get(relaGet);

        ArrayList<Put> inboxPuts = new ArrayList();

        for (Cell cell : result.rawCells()) {

            byte[] fan = CellUtil.cloneQualifier(cell);

            Put inboxPut = new Put(fan);

            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(row));

            inboxPuts.add(inboxPut);
        }

        if (inboxPuts.size() > 0) {

            Table inboxTable = conn.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            inboxTable.put(inboxPuts);

            inboxTable.close();
        }

        relaTable.close();
        connTable.close();
        conn.close();
    }

    //关注用户
    public static void attentionAttends(String uid, String... attends) throws IOException {

        if (attends.length == 0) {
            System.out.println("没有设置关注的用户");
            return;
        }

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作关系表
        Table relaTable = conn.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        Put uidPut = new Put(Bytes.toBytes(uid));

        ArrayList<Put> relaPuts = new ArrayList();

        for (String attend : attends) {

            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

            Put attendPut = new Put(Bytes.toBytes(attend));

            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));

            relaPuts.add(attendPut);

        }

        relaPuts.add(uidPut);

        relaTable.put(relaPuts);


        //第二部分：操作收件箱表   B关注了一批用户，将会把这批用户的2条微博显示给B
        Table conTable = conn.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Put uidInboxPut = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {

            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));

            ResultScanner resultScanner = conTable.getScanner(scan);

            //ts++是为了防止每条数据的时间戳一样导致最后插了进去一条
            long ts = System.currentTimeMillis();


            for (Result result : resultScanner) {

                int i = 0;

                Cell[] cells = result.rawCells();

                uidInboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, CellUtil.cloneRow(cells[i++]));

                if (i >= 1) {
                    break;
                }
            }


        }

        if (!uidInboxPut.isEmpty()) {

            Table inboxTable = conn.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            inboxTable.put(uidInboxPut);

            inboxTable.close();
        }

        relaTable.close();
        conTable.close();
        conn.close();
    }

    //取关用户
    public static void abolishAttends(String uid, String... attends) throws IOException {

        if (attends.length == 0) {
            System.out.println("没有设置关注的用户");
            return;
        }

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作用户关系表
        Table relaTable = conn.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Delete> deletes = new ArrayList<Delete>();

        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        for (String attend : attends) {

            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend));

            Delete attendDelete = new Delete(Bytes.toBytes(attend));

            attendDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));

            deletes.add(attendDelete);
        }

        deletes.add(uidDelete);

        relaTable.delete(deletes);

        //第二部分：操作收件箱表
        Table inboxTable = conn.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        Delete delete = new Delete(Bytes.toBytes(uid));

        for (String attend : attends) {

            delete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend));
        }

        inboxTable.delete(delete);

        relaTable.close();
        inboxTable.close();
        conn.close();


    }

    //获取用户初始化页面
    public static void getInit(String uid) throws IOException {

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table inboxTable = conn.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        Table conTable = conn.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Get get = new Get(Bytes.toBytes(uid));

        get.setMaxVersions();

        Result result = inboxTable.get(get);

        for (Cell cell : result.rawCells()) {

            Get conget = new Get(CellUtil.cloneRow(cell));

            Result conResult = conTable.get(conget);

            for (Cell conCell : conResult.rawCells()) {
                System.out.println(CellUtil.cloneRow(conCell).toString() + ":" + CellUtil.cloneValue(conCell).toString());
            }

        }

        conTable.close();
        inboxTable.close();
        conn.close();
    }

    //获取指定用户微博详情
    public static void getWeiBo(String uid) throws IOException {

        Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table connTable = conn.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));

        scan.setFilter(rowFilter);

        ResultScanner scanner = connTable.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(CellUtil.cloneRow(cell).toString() + ":" + CellUtil.cloneValue(cell).toString());
            }
        }

        connTable.close();
        conn.close();
    }
}
