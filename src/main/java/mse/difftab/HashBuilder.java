package mse.difftab;

import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

class HashBuilder extends Thread {
	public static final int KEY_POS_LENGTH=6;

	
	private DiffTab app;
	private String srcName;
	private Object[] arr;
	private int arrSz;
	private Hasher[] hasher;
	private int[] dataLength;
	private boolean groupByKey;
	private OutputStream[] hashOutH;
	private OutputStream[] hashOutK;
	private SharedValueLong[] keyFilePos;
	private Semaphore sem;
	private int colsToRead;
	private int[] colOffset;
	private boolean[] isHash;
	private boolean[] isKey;
	private TabInfo ti;
	private String[] hasherClassName;
	private String[] colName;
	
    public HashBuilder(
    	DiffTab app,
    	String srcName,
    	TabInfo ti,
    	Object[] arr,
    	int arrSz,
    	int colsToRead,
    	String[] colName,
    	String[] hasherClassName,
    	int[] colOffset,
    	int[] dataLength,
    	boolean[] isHash,
    	boolean[] isKey,
    	boolean groupByKey,
    	OutputStream[] hashOutH,
    	OutputStream[] hashOutK,
    	SharedValueLong[] keyFilePos,
    	Semaphore sem
    ){
    	this.app = app;
    	this.srcName = srcName;
    	this.ti = ti;
    	this.arr = arr;
    	this.arrSz = arrSz;
    	this.colsToRead = colsToRead;
    	this.colName = colName;
    	this.hasherClassName = hasherClassName;
    	this.colOffset = colOffset;
    	this.dataLength = dataLength;
    	this.isHash = isHash;
    	this.isKey = isKey;
    	this.groupByKey = groupByKey;
    	this.hashOutH = hashOutH;
    	this.hashOutK = hashOutK;
    	this.keyFilePos = keyFilePos;
    	this.sem = sem;
    }
	
	public void run(){
		int currPos = 0;
    	int dataLen;
    	int dataPos;
    	int bucket;
    	int i;

    	MessageDigest mdHash=null;
    	MessageDigest mdKey=null;
		try {
	    	hasher = new Hasher[hasherClassName.length];
			MessageDigest md = Hasher.getMessageDigestInstance();
	    	for(i=0;i<this.hasher.length;i++) {
	   			hasher[i] = (Hasher) Class.forName(hasherClassName[i]).newInstance();
	   			hasher[i].setInfo(ti, colName[i]);
	   			hasher[i].setMessageDigest(md);
	    	}

			mdHash = Hasher.getMessageDigestInstance();
	    	mdKey = Hasher.getMessageDigestInstance();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
    	byte[] hashCol = new byte[Hasher.HASH_LENGTH_FULL];
    	byte[] keyCol = new byte[Hasher.HASH_LENGTH_FULL];
    	byte[] data = new byte[Arrays.stream(dataLength).sum()+dataLength.length*2];
    	boolean hashIsPresent=false;
    	for(boolean b : isHash) hashIsPresent = hashIsPresent || b;
 	
    	int keyFilePosInBufferOutH = Hasher.HASH_LENGTH+(groupByKey?Hasher.HASH_LENGTH:0);
    	byte[] bufferOutH=new byte[keyFilePosInBufferOutH+KEY_POS_LENGTH];
    	keyFilePosInBufferOutH += KEY_POS_LENGTH - 1;
   	
		try {
			sem.acquire();
			
	        while(currPos<arrSz){
	        	dataPos=0;
	        	for(i=0;i<colOffset.length;i++){
		  			if(isHash[i]){
		  				if(isKey[i]){
		  					if(groupByKey){
		  						dataLen=hasher[i].getHashAndData(arr[currPos+colOffset[i]],keyCol,0,data,dataPos,dataLength[i]);
		  						mdKey.update(keyCol);
		  					}else{
		  						dataLen=hasher[i].getHashAndData(arr[currPos+colOffset[i]],hashCol,0,data,dataPos,dataLength[i]);
		  						mdHash.update(hashCol);
		  					}
		  					if(dataLen >= 0) {
		  						if(dataLen<=Hasher.DATA_LEN_VAL_MAX_FOR_1_BYTE){
		  							data[dataPos++]=(byte)dataLen;
		  							dataPos+=dataLen;
		  						}else{
		  							data[dataPos++]=(byte)((dataLen>>8)|(1<<7));
		  							data[dataPos++]=(byte)(dataLen);
		  							dataPos+=dataLen;
		  						}
		  					}else{
			  					if(dataLen==Hasher.DATA_LEN_TO_RETURN_FOR_NULL){
			  						data[dataPos++]=Hasher.DATA_LEN_TO_WRITE_FOR_NULL;
			  					}else if(dataLen==Hasher.DATA_LEN_TO_RETURN_FOR_NO_COLUMN){
			  						data[dataPos++]=Hasher.DATA_LEN_TO_WRITE_FOR_NO_COLUMN;
			  					}else if(dataLen==Hasher.DATA_LEN_TO_RETURN_FOR_NO_SERIALIZER){
			  						data[dataPos++]=Hasher.DATA_LEN_TO_WRITE_FOR_NO_SERIALIZER;
			  					}else{
			  						throw new RuntimeException("Unknown dataLen value \""+dataLen+"\"");
			  					}
			  				}
		  				}else{
		  					hasher[i].getHash(arr[currPos+colOffset[i]],hashCol,0);
		  					mdHash.update(hashCol);
		  				}
		  			}else{
		  				if(isKey[i]){
		  					dataLen=hasher[i].getData(arr[currPos+colOffset[i]],data,dataPos,dataLength[i]);
		  					if(dataLen >= 0) {
		  						if(dataLen<=Hasher.DATA_LEN_VAL_MAX_FOR_1_BYTE){
		  							data[dataPos++]=(byte)dataLen;
		  							dataPos+=dataLen;
		  						}else{
		  							data[dataPos++]=(byte)((dataLen>>8)|(1<<7));
		  							data[dataPos++]=(byte)(dataLen);
		  							dataPos+=dataLen;
		  						}
		  					}else{
			  					if(dataLen==Hasher.DATA_LEN_TO_RETURN_FOR_NULL){
			  						data[dataPos++]=Hasher.DATA_LEN_TO_WRITE_FOR_NULL;
			  					}else if(dataLen==Hasher.DATA_LEN_TO_RETURN_FOR_NO_COLUMN){
			  						data[dataPos++]=Hasher.DATA_LEN_TO_WRITE_FOR_NO_COLUMN;
			  					}else if(dataLen==Hasher.DATA_LEN_TO_RETURN_FOR_NO_SERIALIZER){
			  						data[dataPos++]=Hasher.DATA_LEN_TO_WRITE_FOR_NO_SERIALIZER;
			  					}else{
				  					throw new RuntimeException("Unknown dataLen value \""+dataLen+"\"");
			  					}
			  				}
		  				}
		  			}
//		  			hasher[i].free(arr[currPos+colOffset[i]]);
	        	}

	  			currPos+=colsToRead;
	        	
	        	if(groupByKey){
	        		mdKey.digest(bufferOutH,0,Hasher.HASH_LENGTH);
	        		if(hashIsPresent){
	        			mdHash.digest(bufferOutH,Hasher.HASH_LENGTH,Hasher.HASH_LENGTH);
	        		}

	        	}else{
        			mdHash.digest(bufferOutH,0,Hasher.HASH_LENGTH);
	        	}

	        	bucket=(hashOutH.length==1?0:(((bufferOutH[0]&0x7f)<<24) | (bufferOutH[1]&0xff<<16) | (bufferOutH[2]&0xff<<8) | bufferOutH[3]&0xff)%hashOutH.length);
	        	
	        	long keyFilePos_;
        		synchronized(hashOutK[bucket]){
        			keyFilePos_ = keyFilePos[bucket].value;
	        		hashOutK[bucket].write(data,0,dataPos);
	        		keyFilePos[bucket].value+=dataPos;
	        	}

          		int keyFilePosInBufferOutH_ = keyFilePosInBufferOutH;
          		bufferOutH[keyFilePosInBufferOutH_] = (byte)(keyFilePos_);
		  		bufferOutH[--keyFilePosInBufferOutH_] = (byte)(keyFilePos_ >>= 8);
    			bufferOutH[--keyFilePosInBufferOutH_] = (byte)(keyFilePos_ >>= 8);
    			bufferOutH[--keyFilePosInBufferOutH_] = (byte)(keyFilePos_ >>= 8);
    			bufferOutH[--keyFilePosInBufferOutH_] = (byte)(keyFilePos_ >>= 8);
    			bufferOutH[--keyFilePosInBufferOutH_] = (byte)(keyFilePos_ >>=8);

    			synchronized(hashOutH[bucket]){
    				hashOutH[bucket].write(bufferOutH);
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
