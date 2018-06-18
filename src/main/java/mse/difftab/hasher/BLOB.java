package mse.difftab.hasher;

import java.sql.Blob;
import java.util.Arrays;

import mse.difftab.Hasher;

public class BLOB extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_NULL;
		}else{
			final Blob dataBlob=(java.sql.Blob)o;
			final long blobLength = dataBlob.length();
			if(blobLength == 0){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_EMPTY;
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=blobLength;) {					
					md.update(dataBlob.getBytes(chunkBoundaryLeft,(int)Math.min(blobLength-chunkBoundaryLeft+1,CHUNK_SIZE_BYTES)));
					chunkBoundaryLeft += CHUNK_SIZE_BYTES;
				}
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW;
			}
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		final Blob dataBlob=(java.sql.Blob)o;
		final long blobLength = dataBlob.length();
		final byte[] dataByteArr=dataBlob.getBytes(1,(int)Math.min(blobLength,maxDataLength));
		if(dataByteArr.length>=maxDataLength){
			System.arraycopy(dataByteArr,0,data,dataOffset+(maxDataLength<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2),maxDataLength);
			return maxDataLength;
		}else{
			System.arraycopy(dataByteArr,0,data,dataOffset+(dataByteArr.length<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2),dataByteArr.length);
			return dataByteArr.length;
		}
	}

	@Override
	public int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_NULL;
			return DATA_LEN_TO_RETURN_FOR_NULL;
		}else{
			final Blob dataBlob=(java.sql.Blob)o;
			final long blobLength = dataBlob.length();
			if(blobLength == 0){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_EMPTY;
				return 0;
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=blobLength;) {					
					md.update(dataBlob.getBytes(chunkBoundaryLeft,(int)Math.min(blobLength-chunkBoundaryLeft+1,CHUNK_SIZE_BYTES)));
					chunkBoundaryLeft += CHUNK_SIZE_BYTES;
				}
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW;
			}
			final byte[] dataByteArr=dataBlob.getBytes(1,(int)Math.min(blobLength,maxDataLength));
			if(dataByteArr.length>=maxDataLength){
				System.arraycopy(dataByteArr,0,data,dataOffset+(maxDataLength<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2),maxDataLength);
				return maxDataLength;
			}else{
				System.arraycopy(dataByteArr,0,data,dataOffset+(dataByteArr.length<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2),dataByteArr.length);
				return dataByteArr.length;
			}
		}
	}
	
	protected void free(Object o)throws Exception{
		((java.sql.Clob)o).free();
		super.free(o);
	}
	
	public byte getCompareAs() {
		return COMPARE_AS_RAW;
	}
}
