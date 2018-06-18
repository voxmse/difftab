package mse.difftab.hasher;

import java.util.Arrays;

import mse.difftab.Hasher;

public class BYTEARRAY extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_NULL;
		}else{
			final byte[] byteArr=(byte[])o;
			if(byteArr.length == 0){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_EMPTY;
			}else if(byteArr.length <= CHUNK_SIZE_BYTES){
				md.update(byteArr);
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW;
			}else{
				for(int chunkBoundaryLeft=0;chunkBoundaryLeft<byteArr.length;) {					
					md.update(byteArr,chunkBoundaryLeft,chunkBoundaryLeft+Math.min(byteArr.length-chunkBoundaryLeft,CHUNK_SIZE_BYTES));
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
		byte[] dataByteArr = Hasher.bytesToHex((byte[])o,0,Math.min(maxDataLength,((byte[])o).length/2)).getBytes(idCharset);
		if(dataByteArr.length*2>=maxDataLength){
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
			byte[] byteArr=(byte[])o;
			if(byteArr.length == 0){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW_FOR_EMPTY;
				return 0;
			}else if(byteArr.length <= CHUNK_SIZE_BYTES){
				md.update(byteArr);
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW;
			}else{
				for(int chunkBoundaryLeft=0;chunkBoundaryLeft<byteArr.length;) {					
					md.update(byteArr,chunkBoundaryLeft,chunkBoundaryLeft+Math.min(byteArr.length-chunkBoundaryLeft,CHUNK_SIZE_BYTES));
					chunkBoundaryLeft += CHUNK_SIZE_BYTES;
				}
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_RAW;
			}
			byteArr=null;
			byte[] dataByteArr = Hasher.bytesToHex((byte[])o,0,Math.min(maxDataLength,((byte[])o).length/2)).getBytes(idCharset);
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
		return COMPARE_AS_RAW;
	}
}
