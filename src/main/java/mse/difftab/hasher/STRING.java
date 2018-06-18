package mse.difftab.hasher;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import mse.difftab.Hasher;

public class STRING extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_NULL;
		}else{
			final String dataStr=(java.lang.String)o;
			if(dataStr.isEmpty()){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_EMPTY;
			}else if(dataStr.length() <= CHUNK_SIZE_CHARS){
				md.update(dataStr.getBytes(StandardCharsets.UTF_8));
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING;
			}else{
				for(int chunkBoundaryLeft=0;chunkBoundaryLeft<dataStr.length();) {					
					md.update(dataStr.substring(chunkBoundaryLeft,Math.min(dataStr.length()-chunkBoundaryLeft,CHUNK_SIZE_CHARS)).getBytes(StandardCharsets.UTF_8));
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
		final byte[] dataByteArr=((((String)o).length())<=maxDataLength)?((String)o).getBytes(idCharset):((String)o).substring(0,maxDataLength).getBytes(idCharset);
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
			String dataStr=(java.lang.String)o;
			if(dataStr.isEmpty()){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING_FOR_EMPTY;
				return 0;
			}else if(dataStr.length() <= CHUNK_SIZE_CHARS){
				md.update(dataStr.getBytes(StandardCharsets.UTF_8));
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING;
			}else{
				for(int chunkBoundaryLeft=0;chunkBoundaryLeft<dataStr.length();) {					
					md.update(dataStr.substring(chunkBoundaryLeft,Math.min(dataStr.length()-chunkBoundaryLeft,CHUNK_SIZE_CHARS)).getBytes(StandardCharsets.UTF_8));
					chunkBoundaryLeft += CHUNK_SIZE_CHARS;
				}
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_STRING;
			}
			dataStr=null;
			final byte[] dataByteArr=((((String)o).length())<=maxDataLength)?((String)o).getBytes(idCharset):((String)o).substring(0,maxDataLength).getBytes(idCharset);
			if(dataByteArr.length>=maxDataLength){
				System.arraycopy(dataByteArr,0,data,dataOffset+(maxDataLength<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2),maxDataLength);
				return maxDataLength;
			}else{
				System.arraycopy(dataByteArr,0,data,dataOffset+(dataByteArr.length<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2),dataByteArr.length);
				return dataByteArr.length;
			}
		}
	}
	
	public byte getCompareAs() {
		return COMPARE_AS_STRING;
	}
}
