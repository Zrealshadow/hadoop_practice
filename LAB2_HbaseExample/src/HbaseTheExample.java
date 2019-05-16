/**
 * @program: HbaseExample
 * @description:
 * @author: E1ixir_zzZ
 * @create: 2019-05-14 15:44
 **/
import com.sun.org.glassfish.gmbal.Description;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.List;

public class HbaseTheExample {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args)throws Exception{
        try {
            createTable("Student", new String[]{"Student_ID", "Student_Major", "Student_info", "Student_Name"});
            insertRow("Student","kky","Student_ID","","112");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @Description: init
     * @Param: []
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:4:48 PM
    */

    public static void init(){
        BasicConfigurator.configure();
        configuration=HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection=ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @Description: close
     * @Param: []
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:4:48 PM
    */
    public static void close(){
        try{
            if(admin!=null){
                admin.close();
            }if(connection!=null){
                connection.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    /**
     * @Description: createTable
     * @Param: [myTableName, colFamily]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:4:48 PM
    */
    public static void createTable(String myTableName, String[] colFamily)throws Exception{
        init();
        TableName tableName=TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("this table is exist");
        }else{
            HTableDescriptor hTableDescriptor=new HTableDescriptor(tableName);
            for(String str:colFamily){
                HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("create table success!");
        }
        close();
    }
    
    
    /**
     * @Description: deleteTable
     * @Param: [tableName]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:4:46 PM
     * */

    public static void deleteTable(String tableName) throws  Exception{
        init();
        TableName tn=TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            admin.disableTable(tn);
            admin.deleteTable(tn);
            System.out.println("delete the table");
        }
        else{
            System.out.println("the table is not exist");
        }
        close();
    }
    
    /**
     * @Description: listTables
     * @Param: []
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:4:48 PM
    */
    public static void listTables() throws IOException{
        init();
        HTableDescriptor hTableDescriptors[]=admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    /**
     * @Description: alterColumn
     * @Param: [tablename, colFamily, col]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/16
     * @Time:9:19 AM
    */
    public static void alterColumn(String tablename,String colFamily)throws Exception{
        init();
        TableName tn=TableName.valueOf(tablename);
        if(admin.tableExists(tn)){
            try{
                admin.disableTable(tn);
                HTableDescriptor hTableDescriptor=admin.getTableDescriptor(tn);
                HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(colFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.addColumn(tn,hColumnDescriptor);
//        admin.modifyTable(tn,hTableDescriptor);
                admin.enableTable(tn);
                System.out.println("the Alter is finished!");
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        close();
    }

    /**
     * @Description: deleteColumn
     * @Param: [tablename, colFamily]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/16
     * @Time:10:37 AM
    */

    public static void deleteColumn(String tablename,String colFamily)throws Exception{
        init();
        TableName tn=TableName.valueOf(tablename);
        if(admin.tableExists(tn)){
            try{
                admin.disableTable(tn);
                admin.deleteColumn(tn,colFamily.getBytes());
                admin.enableTable(tn);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        close();
    }

    /**
    *向某一行插入数据 put
    *@param tableName 表名
    *@param rowKey 行键
    *@param colFamily 列族名
    *@param col 列名
    *@param val 值
    * */
    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val)throws Exception{
        init();
        Table table=connection.getTable(TableName.valueOf(tableName));
        Put put=new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
        close();
    }

    /**
     * @Description: deleteRow
     * @Param: [tableName, rowKey, colFamily, col]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:4:50 PM
    */
    public static void deleteRow(String tableName,String rowKey,String colFamily,String col)throws Exception{
        init();
        Table table=connection.getTable(TableName.valueOf(tableName));
        Delete delete=new Delete(rowKey.getBytes());
        //删除指定列族的数据
        if(colFamily!=null) {
            delete.addFamily(colFamily.getBytes());
        }
        //删除指定列的数据
        if(col!=null){
            delete.addColumn(colFamily.getBytes(),col.getBytes());
        }

        table.delete(delete);
        table.close();
        close();
    }

    /**
     * @Description: getData
     * @Param: [tableName, rowKey, colFamily, col]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:5:00 PM
     * */
    public static void getData(String tableName,String rowKey,String colFamily,String col)throws Exception{
        init();
        Table table=connection.getTable(TableName.valueOf(tableName));
        Get get=new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result=table.get(get);
        table.close();
        close();
    }

    /**
     * @Description: displayCell
     * @Param: [result]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:6:54 PM
    */
    private static void displayCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.print("RowName:"+new String(CellUtil.cloneRow(cell))+"\t");
            System.out.print("Timetamp:"+cell.getTimestamp()+" ");
            System.out.print("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.print("column:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }

    }

    /**
     * @Description: clearTable
     * @Param: [tableName]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:7:20 PM
    */
    public static void clearTable(String tableName)throws Exception{
        init();
        TableName tn=TableName.valueOf(tableName);
        if(!admin.tableExists(tn)){
            System.out.println("the table is not exist");
        }
        else{
            HTableDescriptor it=admin.getTableDescriptor(tn);
            admin.disableTable(tn);;
            admin.deleteTable(tn);
            admin.createTable(it);
            System.out.println(tableName+" is being format");
        }
        close();
    }

    /**
     * @Description: showDetailsOfTable
     * @Param: [tableName]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:7:40 PM
    */
    public static void showDetailsOfTable(String tableName)throws Exception{
        init();
        TableName tablename=TableName.valueOf(tableName);
        Table table=connection.getTable(tablename);
        Scan scan=new Scan();
        ResultScanner scanner=table.getScanner(scan);
        System.out.println("**************Table:"+tableName+"**************");
        for(Result result=scanner.next();result!=null;result=scanner.next())
            displayCell(result);
        System.out.println("***********************************************");
        scanner.close();
        table.close();
        close();
    }

    /**
     * @Description: listAllTable
     * @Param: []
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:8:35 PM
    */
    public static void listAllTable()throws Exception{
        init();
        HTableDescriptor[] hTableDescriptors=admin.listTables();
        // iterator 列表
        for(HTableDescriptor hTableDescriptor:hTableDescriptors) {
            TableName tn = hTableDescriptor.getTableName();
            System.out.println("Table Name:" + tn.toString());
            HColumnDescriptor[] hColumnDescriptors = hTableDescriptor.getColumnFamilies();
            System.out.println("the Column:\n{");
            //iterator 列族
            for (HColumnDescriptor hColumnDescriptor : hColumnDescriptors) {
                String column = hColumnDescriptor.getNameAsString();
                System.out.println(column);
            }
            System.out.println("}");
        }
        close();
    }

    /**
     * @Description: countRowOfTable
     * @Param: [tableName]
     * @return void
     * @Author: Zreal
     * @Date:2019/5/14
     * @Time:8:35 PM
    */

    public static void countRowOfTable(String tableName)throws Exception{
        init();
        TableName tablename=TableName.valueOf(tableName);
        Table table=connection.getTable(tablename);
        Scan scan=new Scan();
        ResultScanner scanner=table.getScanner(scan);
        int sum=0;
        for(Result result=scanner.next();result!=null;result=scanner.next())
            sum+=1;
        System.out.println("the number of rows in table "+tableName+":"+sum);
        table.close();
        close();
    }


}
