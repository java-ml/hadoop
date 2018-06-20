package Hbsae;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;



public class HbaseApi {
	private Connection conn;
	@Before
	public void iniConn() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		
		conn=ConnectionFactory.createConnection(conf);
	}
	@Test
	public void dropTable(String tablename) throws Exception {
		Admin admin=conn.getAdmin();
		
		//禁用表
		admin.disableTable(TableName.valueOf(Bytes.toBytes(tablename)));
		//删除表
		admin.deleteTable(TableName.valueOf(Bytes.toBytes(tablename)));
		admin.close();
	}
	@Test
	public void createTable(String namespace,String tab,String col) throws Exception {
		Admin admin=conn.getAdmin();
		//创建名字空间
		NamespaceDescriptor.Builder builder=NamespaceDescriptor.create(namespace);
		NamespaceDescriptor nDescriptor=builder.build();
		admin.createNamespace(nDescriptor);
		//表描述符
		HTableDescriptor desc=new HTableDescriptor(TableName.valueOf(namespace+":"+tab));
		//列族描述符
		HColumnDescriptor colDesc=new HColumnDescriptor(col.getBytes());
		//添加列族
		desc.addFamily(colDesc);
		admin.createTable(desc);
		admin.close();
	}
	public void putData(String tab,String row,String col,String cols,String value) throws Exception {
		Table t=conn.getTable(TableName.valueOf(tab.getBytes()));
		Put put =new Put(row.getBytes());
		put.addColumn(col.getBytes(), cols.getBytes(), value.getBytes());
		t.put(put);
		t.close();
	}
	public void Del() throws Exception {
		Table t=conn.getTable(TableName.valueOf("t1".getBytes()));
		Delete delete=new Delete("row1".getBytes());
		delete.addColumn("l1".getBytes(), "".getBytes());
		t.delete(delete);
	}
	public void getData() throws Exception {
		Table t=conn.getTable(TableName.valueOf("t1".getBytes()));
		Get get =new Get("row1".getBytes());
		get.addFamily("l1".getBytes());
		Result res=t.get(get);
		res.getValue("l1".getBytes(), "".getBytes());
	}
	public static void main(String[] args) {
		

	}

}
