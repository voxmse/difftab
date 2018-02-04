package mse.difftab.hasher;

import java.sql.Blob;
import mse.difftab.Hasher;

public class BLOB extends Hasher {
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}
	
	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			final Blob dataBlob=(java.sql.Blob)o;
			final long blobLength = dataBlob.length();
			if(blobLength == 0){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=blobLength;) {					
					md.update(dataBlob.getBytes(chunkBoundaryLeft,(int)Math.min(blobLength-chunkBoundaryLeft+1,CHUNK_SIZE_BYTE)));
					chunkBoundaryLeft += CHUNK_SIZE_BYTE;
				}
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(o==null) return -1;
		final Blob dataBlob=(java.sql.Blob)o;
		final long blobLength = dataBlob.length();
		final byte[] dataByteArr=dataBlob.getBytes(1,(int)Math.min(blobLength,maxDataLength));
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
			final Blob dataBlob=(java.sql.Blob)o;
			final long blobLength = dataBlob.length();
			if(blobLength == 0){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=blobLength;) {					
					md.update(dataBlob.getBytes(chunkBoundaryLeft,(int)Math.min(blobLength-chunkBoundaryLeft+1,CHUNK_SIZE_BYTE)));
					chunkBoundaryLeft += CHUNK_SIZE_BYTE;
				}
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
			final byte[] dataByteArr=dataBlob.getBytes(1,(int)Math.min(blobLength,maxDataLength));
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
