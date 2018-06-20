import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class hadoopApi {

	
	
	@Test
	public  void readFile() throws Exception {
		//URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
		Configuration con =new Configuration();
		con.set("fs.defaultFS", "hdfs://localhost:9000/");
		FileSystem fs =FileSystem.get(con);
		FSDataInputStream fis = fs.open(new Path("/hadoop/input/text.txt"));
		FSDataOutputStream fsw=fs.append(new Path("/hadoop/input/text.txt"));
		
		byte [] buf =new byte[1024];
		int len =-1;
		ByteArrayOutputStream baos =new ByteArrayOutputStream();
		
		while( (len= fis.read(buf) )!= -1){
			//baos.write(buf,0,len);
			System.out.print(new String(buf,0,len));
		}
		fis.close();
		baos.close();
		//System.out.print(new String(baos.toByteArray()));
	}
	
	@Test
	public  void readFilehome() throws Exception {
		//URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Configuration con =new Configuration();
		con.set("fs.defaultFS", "hdfs://localhost:9000/");
		FileSystem fs =FileSystem.get(con);
				//FSDataInputStream fis = fs.open(new Path("/hadoop/input/text.txt"));
		
		
		FileStatus[] status= fs.listStatus(new Path("/hadoop"));
		for (FileStatus status2:status) {
			System.out.println(status2.getPath());
			
		}
		/*
		Path[] paths =FileUtil.stat2Paths(status);
		for(int i=0;i<paths.length;i++)System.out.println(paths[i]);
		*/
	}
	@Test
	public void  intWiter() throws Exception {
		Text iw= new Text();
		iw.set("l在");
		ByteArrayOutputStream baos =new ByteArrayOutputStream();
		iw.write(new DataOutputStream(baos));
		baos.close();
		
		//iw.readFields(new DataInputStream(new ByteArrayInputStream(b)));
		byte []b=iw.getBytes();
		System.out.println(iw.getLength()+","+iw.getBytes().length+"\\"+b.length);
		for(byte bh:b)System.out.println(bh);
	}
	@Test
	public void ObjW5iter() throws Exception{
		IntWritable iWritable =new IntWritable();
		iWritable.set(139);
		ByteArrayOutputStream baos =new ByteArrayOutputStream();
		iWritable.write(new DataOutputStream(baos));
		byte[] b=baos.toByteArray();
		DataInputStream dStream=new DataInputStream(new ByteArrayInputStream(b));
		System.out.println(dStream.readInt());
		
	}
	
	/*public void AvroW() throws Exception{
		StringP sP=new StringP();
		sP.setLegth("k");
		sP.setRight(0); 
		ByteArrayOutputStream out =new ByteArrayOutputStream();
		DatumWriter<StringP> writer = new  SpecificDatumWriter<StringP>(StringP.class);
		Encoder encoder= EncoderFactory.get().binaryEncoder(out, null);
		writer.write(sP, encoder);
		encoder.flush();
		out.close();
		DatumReader<StringP> reader=new SpecificDatumReader<StringP>(StringP.class);
		Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		StringP sP2=reader.read(null, decoder);
		System.out.print(sP2.getLegth()+"."+sP2.getRight());
	}  
*/	@Test
	public void SeqenceFile () throws Exception{
	
	Configuration con =new Configuration();
	con.set("fs.defaultFS", "hdfs://localhost:9000/");
	FileSystem fsSystem=FileSystem.get(con);
	
	Writer writer=SequenceFile.createWriter(fsSystem, con, new Path("/user/hadoop/sefile.seq"), 
			Text.class, IntWritable.class);
	Random random=new Random();
	for(int i=0;i<=200;i++){
	
		writer.append(new Text("190"+""+random.nextInt(10)),new IntWritable(random.nextInt(30)));
		}
	writer.append(new Text("1901"),new IntWritable(33));
	writer.append(new Text("1902"),new IntWritable(38));
	writer.append(new Text("1903"),new IntWritable(45));
	writer.append(new Text("1904"),new IntWritable(35));
	writer.append(new Text("1905"),new IntWritable(39));
	writer.append(new Text("1906"),new IntWritable(41));
	writer.append(new Text("1907"),new IntWritable(53));
	writer.append(new Text("1908"),new IntWritable(36));
	writer.close();
	/*
	SequenceFile.Reader reader = new SequenceFile.Reader(fsSystem, new Path("/user/hadoop/sefile.seq"), con);
	IntWritable writable =new IntWritable();
	Text        vel      =new Text();
	while(reader.next(writable,vel)){
	//	reader.getCurrentValue(vel);
	
	    System.out.println(writable.get());
	  } 
	  */
}
	

}

 