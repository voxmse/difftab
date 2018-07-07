package mse.difftab.hasher;

import mse.difftab.Hasher;
import java.util.Arrays;

public class BSON_OBJECTID_USING_REFLECTION extends Hasher {
	@SuppressWarnings("rawtypes")
	private final Class[] noparms = {};
	
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID_FOR_NULL;
		}else{
			final byte[] value=(byte[])o.getClass().getDeclaredMethod("toByteArray",noparms).invoke(o);
			if(value.length>0){
				md.update(value);
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID;
			}else{
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID_FOR_EMPTY;
			}
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset,int maxDataLength)throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		final byte[] dataByteArr=o.toString().getBytes(idCharset);
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
			final byte[] value=(byte[])o.getClass().getDeclaredMethod("toByteArray",noparms).invoke(o);
			if(value.length>0){
				md.update(value);
				md.digest(hash, hashOffset, HASH_LENGTH);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID;
			}else{
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_ID_FOR_EMPTY;
				return 0;
			}
			final byte[] dataByteArr=o.toString().getBytes(idCharset);
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
