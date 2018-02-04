package mse.difftab;

import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.concurrent.Semaphore;

class HashBuilder extends Thread {
	public static final int KEY_POS_LENGTH=6;
	
	private DiffTab app;
	private String srcName;
	private int colNum;
	private Object[] arr;
	private int arrSz;
	private int hashColsSz;
	private int keyColsSz;
	private int dataSz;
	private int[] hashIdx;
	private int[] keyIdx;
	private Hasher[] hasher;
	private int[] hashOffset;
	private int[] keyOffset;
	private int[] dataLength;
	private boolean groupByKey;
	private OutputStream[] hashOutH;
	private OutputStream[] hashOutK;
	private SharedValueLong[] keyFilePos;
	private Semaphore sem;
	private MessageDigest md;
	
    public HashBuilder(
    	DiffTab app,
    	String srcName,
    	int colNum,
    	Object[] arr,
    	int arrSz,
    	int hashColsSz,
    	int keyColsSz,
    	int dataSz,
    	int[] hashIdx,
    	int[] keyIdx,
    	String[] hasherClassName,
    	int[] hashOffset,
    	int[] keyOffset,
    	int[] dataLength,
    	boolean groupByKey,
    	OutputStream[] hashOutH,
    	OutputStream[] hashOutK,
    	SharedValueLong[] keyFilePos,
    	Semaphore sem
    )throws Exception{
    	this.app = app;
    	this.srcName = srcName;
    	this.colNum = colNum;
    	this.arr = arr;
    	this.arrSz = arrSz;
    	this.hashColsSz = hashColsSz;
    	this.keyColsSz = keyColsSz;
    	this.dataSz = dataSz;
    	this.hashIdx = hashIdx;
    	this.keyIdx = keyIdx;
    	this.hasher = new Hasher[hasherClassName.length];
    	this.md = Hasher.getMessageDigestInstance();
    	for(int i=0;i<this.hasher.length;i++) {
    		hasher[i] = (Hasher) Class.forName(hasherClassName[i]).newInstance();
    		hasher[i].setMessageDigest(md);
    	}
    	this.hashOffset = hashOffset;
    	this.keyOffset = keyOffset;
    	this.dataLength = dataLength;
    	this.groupByKey = groupByKey;
    	this.hashOutH = hashOutH;
    	this.hashOutK = hashOutK;
    	this.keyFilePos = keyFilePos;
    	this.sem = sem;
    }
	
	public void run(){
		int i;
		int currPos = 0;
    	int dataLen;
    	int dataPos;
    	long keyFilePos_;
    	int bucket;
    	
    	byte[] hashCols = new byte[hashColsSz];
    	byte[] keyCols = new byte[keyColsSz];
    	byte[] data = new byte[dataSz];
 	
    	int keyFilePosInBufferOutH=Hasher.HASH_LENGTH+(groupByKey?Hasher.HASH_LENGTH:0);
    	byte[] bufferOutH=new byte[keyFilePosInBufferOutH+KEY_POS_LENGTH];
   	
		try {
			sem.acquire();
			
	        while(currPos<arrSz){
	        	dataPos=0;
	        	for(i=0;i<colNum;i++){
		  			if(hashIdx[i]>0){
		  				if(keyIdx[i]>0){
		  					if(groupByKey){
		  						dataLen=hasher[i].getHashAndData(arr[currPos],keyCols,keyOffset[i],data,dataPos,dataLength[i]);
		  					}else{
		  						dataLen=hasher[i].getHashAndData(arr[currPos],hashCols,hashOffset[i],data,dataPos,dataLength[i]);
		  					}
		  					if(dataLen==-1){
		  						data[dataPos++]=(byte)0x7f;
		  					}else if(dataLen<127){
		  						data[dataPos++]=(byte)dataLen;
		  						dataPos+=dataLen;
		  					}else{
			  					data[dataPos++]=(byte)((dataLen>>8)|(1<<7));
			  					data[dataPos++]=(byte)(dataLen);
			  					dataPos+=dataLen;
			  				}
		  				}else{
		  					hasher[i].getHash(arr[currPos],hashCols,hashOffset[i]);
		  				}
		  			}else{
		  				if(keyIdx[i]>0){
		  					dataLen=hasher[i].getData(arr[currPos],data,dataPos,dataLength[i]);
		  					if(dataLen==-1){
		  						data[dataPos++]=(byte)0x7f;
		  					}else if(dataLen<127){
		  						data[dataPos++]=(byte)dataLen;
		  						dataPos+=dataLen;
		  					}else{
			  					data[dataPos++]=(byte)((dataLen>>8)|(1<<7));
			  					data[dataPos++]=(byte)(dataLen);
			  					dataPos+=dataLen;
			  				}
		  				}
		  			}
		  			hasher[i].free(arr[currPos]);
		  			currPos++;
	        	}
	
	        	if(groupByKey){
	        		md.update(keyCols);
	        		md.digest(bufferOutH,0,Hasher.HASH_LENGTH);
	        		if(hashCols.length>0){
	        			md.update(hashCols);
	        			md.digest(bufferOutH,Hasher.HASH_LENGTH,Hasher.HASH_LENGTH);
	        		}
		        	bucket=Hasher.getHashBacket(bufferOutH,hashOutH.length);
	        	}else{
        			md.update(hashCols);
        			md.digest(bufferOutH,0,Hasher.HASH_LENGTH);
        			bucket=Hasher.getHashBacket(bufferOutH,hashOutH.length);
	        	}
	        	
        		synchronized(keyFilePos[bucket]){
        			keyFilePos_=keyFilePos[bucket].value;
        			bufferOutH[keyFilePosInBufferOutH]=(byte)(keyFilePos_>>40);
        			bufferOutH[keyFilePosInBufferOutH+1]=(byte)(keyFilePos_>>32);
        			bufferOutH[keyFilePosInBufferOutH+2]=(byte)(keyFilePos_>>24);
        			bufferOutH[keyFilePosInBufferOutH+3]=(byte)(keyFilePos_>>16);
        			bufferOutH[keyFilePosInBufferOutH+4]=(byte)(keyFilePos_>>8);
        			bufferOutH[keyFilePosInBufferOutH+5]=(byte)(keyFilePos_);
    	        	hashOutH[bucket].write(bufferOutH);
	        		hashOutK[bucket].write(data,0,dataPos);
	        		keyFilePos[bucket].value+=dataPos;
	        	}
	        }
		}catch(Exception e){
	    	e.printStackTrace();
	    	app.logError("hash fatal error:"+srcName+":"+e.getMessage());
	    	app.registerFailure(new RuntimeException("hash fatal error:"+srcName+":"+e.getMessage(),e));
		}finally{
			sem.release();
		}
	}
}
