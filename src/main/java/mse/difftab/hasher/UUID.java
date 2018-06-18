package mse.difftab.hasher;

import mse.difftab.Hasher;
import java.util.Arrays;

public class UUID extends Hasher {
	private final byte[] value = new byte[128];
	
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID_FOR_NULL;
		}else{
			Hasher.longToByteArray(((java.util.UUID)o).getMostSignificantBits(),value,0);
			Hasher.longToByteArray(((java.util.UUID)o).getLeastSignificantBits(),value,64);
			md.update(value);
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID;
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset,int maxDataLength)throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		final byte[] dataByteArr=((java.util.UUID)o).toString().getBytes(idCharset);
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
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID_FOR_NULL;
			return DATA_LEN_TO_RETURN_FOR_NULL;
		}else{
			Hasher.longToByteArray(((java.util.UUID)o).getMostSignificantBits(),value,0);
			Hasher.longToByteArray(((java.util.UUID)o).getLeastSignificantBits(),value,64);
			md.update(value);
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID;
			final byte[] dataByteArr=((java.util.UUID)o).toString().getBytes(idCharset);
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
		return COMPARE_AS_ID;
	}
}
