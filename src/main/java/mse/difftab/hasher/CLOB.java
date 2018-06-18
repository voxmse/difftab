package mse.difftab.hasher;

import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.util.Arrays;

import mse.difftab.Hasher;


public class CLOB extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_NULL;
		}else{
			final Clob dataClob=(java.sql.Clob)o;
			final long clobLength = dataClob.length();
			if(clobLength == 0){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_EMPTY;
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=clobLength;) {					
					md.update(dataClob.getSubString(chunkBoundaryLeft,(int)Math.min(clobLength-chunkBoundaryLeft+1,CHUNK_SIZE_CHARS)).getBytes(StandardCharsets.UTF_8));
					chunkBoundaryLeft += CHUNK_SIZE_CHARS;
				}
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING;
			}
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		final Clob dataClob=(java.sql.Clob)o;
		final long clobLength = dataClob.length();
		final byte[] dataByteArr=dataClob.getSubString(1,(int)Math.min(clobLength,maxDataLength)).getBytes(idCharset);
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
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_NULL;
			return DATA_LEN_TO_RETURN_FOR_NULL;
		}else{
			final Clob dataClob=(java.sql.Clob)o;
			final long clobLength = dataClob.length();
			if(clobLength == 0){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_EMPTY;
				return 0;
			}else{
				for(int chunkBoundaryLeft=1;chunkBoundaryLeft<=clobLength;) {					
					md.update(dataClob.getSubString(chunkBoundaryLeft,(int)Math.min(clobLength-chunkBoundaryLeft+1,CHUNK_SIZE_CHARS)).getBytes(StandardCharsets.UTF_8));
					chunkBoundaryLeft += CHUNK_SIZE_CHARS;
				}
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING;
			}
			final byte[] dataByteArr=dataClob.getSubString(1,(int)Math.min(clobLength,maxDataLength)).getBytes(idCharset);
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
		return COMPARE_AS_STRING;
	}
}
