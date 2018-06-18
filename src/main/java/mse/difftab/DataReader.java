package mse.difftab;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
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

	// translate logical to physical column's position per column
	private int colOffset[];
	
	// Column names 
	private String colName[];
	// Hasher's class name per column
	private String hasherClassName[];
	// key columns sign
	private boolean isKey[];
	// hash columns sign
	private boolean isHash[];
	// Max data length per column
	private int dataLength[];
	
	// trace time(ms)
	private int trace;
	
	private Semaphore sem;
	
	public DataReader(String srcName,DiffTab app,ResultSet rs,TabInfo ti,int chunkSize,boolean groupByKey,ExecutorService hes,Semaphore sem,OutputStream[] hashOutH,OutputStream[] hashOutK,SharedValueLong[] keyFilePos,int trace)throws Exception{
		this.srcName = srcName;
		this.app = app;
		this.rs = rs;
		this.ti = ti;
		this.chunkSize = chunkSize;
		
		this.groupByKey = groupByKey;
		this.hes = hes;
		this.hashOutH = hashOutH;
		this.hashOutK = hashOutK;
		this.keyFilePos = keyFilePos;
		this.trace = trace;
		
		this.sem = sem;
		
		// allocate memory for columns' hashes and data hashers
		int colNum = (int)ti.columns.values().stream().filter(c -> c.hashIdx>0 || c.keyIdx>0).count();
		colOffset=new int[colNum];
		hasherClassName = new String[colNum];
		colName = new String[colNum];
		isKey = new boolean[colNum];
		isHash = new boolean[colNum];
		dataLength = new int[colNum];
		
		int i=0;
		// order by alias(logical order)
		for(ColInfo col : ti.columns.values().stream().filter(c -> c.hashIdx>0 || c.keyIdx>0).sorted((c1,c2) -> c1.alias.compareTo(c2.alias)).collect(Collectors.toList())){
			colOffset[i] = col.colIdx-1;
			isHash[i] = (col.hashIdx>0);
			isKey[i] = (col.keyIdx>0);
			dataLength[i] = col.keyIdx>0?app.getMaxKeyColSize():0;
			colName[i] = col.dbName;
			hasherClassName[i]=col.hasherClassName;
			i++;
		}
	}
	
	public void run(){
		long lastTs = System.currentTimeMillis();
		long currTs;
		long rowsProcessed = 0;

        app.writeLog("read:"+srcName+":started");
		
        //Read data
        try{
    	  int colNum = ti.columns.size();
    	  int arrSz = colNum*chunkSize;
          Object[] arr = new Object[arrSz];
          int currPos = 0;
     
          while(rs.next()){
        	for(int i=1;i<=colNum;i++)	arr[currPos++]=rs.getObject(i);
            if(currPos==arrSz){
 	          if(!app.checkFailure())break;
 	          hes.submit(new HashBuilder(app,srcName,ti,arr,arrSz,colNum,colName,hasherClassName,colOffset,dataLength,isHash,isKey,groupByKey,hashOutH,hashOutK,keyFilePos,sem));
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
            hes.submit(new HashBuilder(app,srcName,ti,arr,currPos,colNum,colName,hasherClassName,colOffset,dataLength,isHash,isKey,groupByKey,hashOutH,hashOutK,keyFilePos,sem));
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
