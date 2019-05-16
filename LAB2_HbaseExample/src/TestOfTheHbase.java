import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import java.awt.geom.Ellipse2D;

/**
 * @program: HbaseExample
 * @description:
 * @author: E1ixir_zzZ
 * @create: 2019-05-14 20:38
 **/
public class TestOfTheHbase {

    public static void main(String[] args)throws Exception{
        TestOfTheHbase test=new TestOfTheHbase();
//        test.Test_CreateTable();
//        test.Test_deleteTable();
//          test.Test_insertRow();
//        test.Test_listAllTables();
//        test.Test_printDetailsOfTable();
//        test.Test_AlterColumnFamily();
//        test.Test_DeleteColumnFamily();
        test.Test_Counting();
    }





    /**
     * @Description: Test_CreateTable
     * @Param: []
     * @return void
     * @Author: Zreal
     * @Date:2019/5/16
     * @Time:8:19 AM
    */
    public void Test_CreateTable(){
//        String tablename="student";
//        String[] columns=new String[]{"Student_ID","Student_Major","Student_Name","Student_Info"};
        String tablename="members";
        String[]columns=new String[]{"Member_ID","Member_Major","Member_Name","Member_Info"};
        try{
            HbaseTheExample.createTable(tablename,columns);
            HbaseTheExample a=new HbaseTheExample();
            a.init();
            boolean ans=a.admin.tableExists(TableName.valueOf(tablename));
            System.out.print(ans);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @Description: Test_deleteTable
     * @Param: []
     * @return void
     * @Author: Zreal
     * @Date:2019/5/16
     * @Time:8:19 AM
    */
    public void Test_deleteTable(){
        String tablename="Student";
        try{
            HbaseTheExample.deleteTable("Student");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public void Test_insertRow()throws Exception{
        String tablename="student";
        try{
            HbaseTheExample.insertRow(tablename,"John","Student_ID","","1120162062");
            HbaseTheExample.insertRow(tablename,"John","Student_Major","","softengineering");
            HbaseTheExample.insertRow(tablename,"John","Student_Name","","John");
            HbaseTheExample.insertRow(tablename,"Jonh","Student_Info","","1234");
//            HbaseTheExample.insertRow(tablename,"Jonh","Student_info","","No.3");
            HbaseTheExample.insertRow(tablename,"Hai","Student_ID","","1120162063");
            HbaseTheExample.insertRow(tablename,"Hai","Student_Major","","Design");
            HbaseTheExample.insertRow(tablename,"Hai","Student_Name","","YunHai");
            HbaseTheExample.insertRow(tablename,"Hai","Student_Info","","lks");
            HbaseTheExample.insertRow(tablename,"Mu","Student_ID","","1120162082");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void Test_listAllTables() throws Exception{
        try{
            HbaseTheExample.listAllTable();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void Test_printDetailsOfTable()throws Exception{
        String tablename="student";
        try{
            HbaseTheExample.showDetailsOfTable(tablename);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void Test_AlterColumnFamily()throws Exception{
        String tablename="members";
        try{
            HbaseTheExample.alterColumn(tablename,"Members_sex");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public  void Test_DeleteColumnFamily()throws Exception{
        String tablename="members";
        HbaseTheExample.deleteColumn("members","Members_sex");
    }


    public void Test_Counting() throws Exception{
        String tablename="student";
        HbaseTheExample.countRowOfTable(tablename);
    }

    public void Test_ClearTheTable()throws Exception{
        String tablename="Student";
        HbaseTheExample.clearTable(tablename);
    }


}
