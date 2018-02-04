package mse.difftab;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.io.OutputStream;
import java.sql.ResultSet;

public class DataReader extends Thread {
	// name of source
	private String srcName;
	// the root app object
	private DiffTab app;

	// tab info
	private TabInfo ti;
	
	// selected data
	private ResultSet rs;
	// number of selected columns
	private int colNum;

	// GROUP BY rows and then compare group-by-group
	private boolean groupByKey;

	// N of rows to process at once
	private int chunkSize;
	
	// hash builder executor service
	private ExecutorService hes;

	// output stream for rows' hashes
	private OutputStream[] hashOutH;
	// output stream for serialized key columns' data
	private OutputStream[] hashOutK;
	// is used to synch parallel hash builders
	private SharedValueLong[] keyFilePos;

	// translate physical to logical column's position per column
	private int keyIdx[];
	// translate physical to logical column's position per column
	private int hashIdx[];
	
	// Hasher's class name per column
	private String hasherClassName[];
	// column's hash offset in the "keyCols" buffer per column
	private int keyOffset[];
	// column's hash offset in the "hashCols" buffer per column
	private int hashOffset[];
	// Max data length per column
	private int dataLength[];
	
	// buffer size for columns' hashes of key columns if groupByKey=true or all hashable columns otherwise
	private int keyColsSz;
	// buffer size for column hashes of non-key columns if groupByKey=true
	private int hashColsSz;
	// buffer size for serialized data of key columns
	private int dataSz;
	// trace time(ms)
	private int trace;
	
	private Semaphore sem;
	
	public DataReader(String srcName,DiffTab app,ResultSet rs,TabInfo ti,int chunkSize,boolean groupByKey,ExecutorService hes,Semaphore sem,OutputStream[] hashOutH,OutputStream[] hashOutK,SharedValueLong[] keyFilePos,int trace)throws Exception{
		this.srcName=srcName;
		this.app=app;
		this.rs=rs;
		this.ti = ti;
		this.chunkSize=chunkSize;
		
		this.groupByKey=groupByKey;
		this.hes=hes;
		this.hashOutH=hashOutH;
		this.hashOutK=hashOutK;
		this.keyFilePos=keyFilePos;
		this.trace=trace;
		
		this.sem=sem;
		
		// allocate memory for columns' hashes and data hashers
		colNum=ti.columns.size();
		keyIdx=new int[colNum];
		hashIdx=new int[colNum];
		hasherClassName=new String[colNum];
		keyOffset=new int[colNum];
		hashOffset=new int[colNum];
		dataLength=new int[colNum];
		
		int i=0;
		// sort by the column position in the result set and iterate
		for(ColInfo col : ti.columns.values().stream().sorted((c1,c2)->c1.colIdx-c2.colIdx).collect(Collectors.toList())){
			hashIdx[i]=col.hashIdx;
			keyIdx[i]=col.keyIdx;
			if(col.keyIdx>0) dataLength[i]=col.dataLength;
			if(groupByKey && col.keyIdx>0){
				keyOffset[i]=Hasher.HASH_LENGTH*(col.keyIdx-1);
			}else if(col.hashIdx>0){
				hashOffset[i]=Hasher.HASH_LENGTH*(col.hashIdx-1);
			}
			if(col.keyIdx>0||col.hashIdx>0) hasherClassName[i]="mse.difftab.hasher."+col.hasherClassName;
			i++;
		}
		// allocate buffers' memory
		keyColsSz=Arrays.stream(keyOffset).max().orElse(-Hasher.HASH_LENGTH)+Hasher.HASH_LENGTH;
		hashColsSz=Arrays.stream(hashOffset).max().orElse(-Hasher.HASH_LENGTH)+Hasher.HASH_LENGTH;
		dataSz=IntStream.rangeClosed(0,colNum-1).map(idx -> keyIdx[idx]>0?dataLength[idx]+2:0).sum();		
	}
	
	public void run(){
		long lastTs = System.currentTimeMillis();
		long currTs;
		
		long rowsProcessed = 0;

        app.writeLog("read:"+srcName+":started");
		
        //Read data
        try{
          int arrSz=colNum*chunkSize;
          Object[] arr = new Object[arrSz];

          int currPos=0;
     
          while(rs.next()){
        	for(int i=1;i<=colNum;i++)	arr[currPos++]=rs.getObject(i);
            if(currPos==arrSz){
 	          if(!app.checkFailure())break;
 	          hes.submit(new HashBuilder(app,srcName,colNum,arr,arrSz,hashColsSz,keyColsSz,dataSz,hashIdx,keyIdx,hasherClassName,hashOffset,keyOffset,dataLength,groupByKey,hashOutH,hashOutK,keyFilePos,sem));
 	          arr=new Object[arrSz];   	
              currPos=0;
    	      rowsProcessed+=arrSz/colNum;

  	    	  if(trace>0){
 	            currTs=System.currentTimeMillis();
  	    	    if((currTs-lastTs)>=trace){
  	              app.writeLog("read:"+srcName+":"+rowsProcessed+" rows");
  	         	  lastTs=currTs;
  	    	    }
  	    	  }
   	        }
          }
          //The last batch
          if(currPos>0 && app.checkFailure()){
            hes.submit(new HashBuilder(app,srcName,colNum,arr,currPos,hashColsSz,keyColsSz,dataSz,hashIdx,keyIdx,hasherClassName,hashOffset,keyOffset,dataLength,groupByKey,hashOutH,hashOutK,keyFilePos,sem));
  	        rowsProcessed+=currPos/colNum;
          }
          
          // update TabInfo row counter
          ti.rows = rowsProcessed;

          app.writeLog("read:"+srcName+":finished:"+rowsProcessed+" rows");
        }catch(Exception e){
          app.logError("read:"+srcName+":read data error: "+e.getMessage());
          app.registerFailure(e);
        }
	}
}
