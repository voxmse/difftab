package mse.difftab.hasher;

import java.nio.charset.StandardCharsets;
import java.sql.Clob;

import mse.difftab.Hasher;

public class CLOB extends Hasher {
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}
	
	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			final Clob dataClob=(java.sql.Clob)o;
			final long clobLength = dataClob.length();
			if(clobLength == 0){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=clobLength;) {					
					md.update(dataClob.getSubString(chunkBoundaryLeft,(int)Math.min(clobLength-chunkBoundaryLeft+1,CHUNK_SIZE_CHAR)).getBytes(StandardCharsets.UTF_8));
					chunkBoundaryLeft += CHUNK_SIZE_CHAR;
				}
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(o==null) return -1;
		final Clob dataClob=(java.sql.Clob)o;
		final long clobLength = dataClob.length();
		final byte[] dataByteArr=dataClob.getSubString(1,(int)Math.min(clobLength,maxDataLength)).getBytes(idCharset);
		if(dataByteArr.length>=maxDataLength){
			System.arraycopy(dataByteArr,0,data,dataOffset+(maxDataLength<127?1:2),maxDataLength);
			return maxDataLength;
		}else{
			System.arraycopy(dataByteArr,0,data,dataOffset+(dataByteArr.length<127?1:2),dataByteArr.length);
			return dataByteArr.length;
		}
	}

	@Override
	protected int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
			return -1;
		}else{
			final Clob dataClob=(java.sql.Clob)o;
			final long clobLength = dataClob.length();
			if(clobLength == 0){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=clobLength;) {					
					md.update(dataClob.getSubString(chunkBoundaryLeft,(int)Math.min(clobLength-chunkBoundaryLeft+1,CHUNK_SIZE_CHAR)).getBytes(StandardCharsets.UTF_8));
					chunkBoundaryLeft += CHUNK_SIZE_CHAR;
				}
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
			final byte[] dataByteArr=dataClob.getSubString(1,(int)Math.min(clobLength,maxDataLength)).getBytes(idCharset);
			if(dataByteArr.length>=maxDataLength){
				System.arraycopy(dataByteArr,0,data,dataOffset+(maxDataLength<127?1:2),maxDataLength);
				return maxDataLength;
			}else{
				System.arraycopy(dataByteArr,0,data,dataOffset+(dataByteArr.length<127?1:2),dataByteArr.length);
				return dataByteArr.length;
			}
		}
	}
	
	protected void free(Object o)throws Exception{
		((java.sql.Clob)o).free();
		super.free(o);
	}
}
