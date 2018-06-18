package mse.difftab.hasher;


import java.util.Arrays;

import mse.difftab.Hasher;


public class BOOLEAN extends Hasher {
	private byte[] buff=new byte[]{0x0,0x1};

	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BOOLEAN_FOR_NULL;
		}else{
			md.update(buff,((java.lang.Boolean)o).booleanValue()?1:0,1);
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BOOLEAN;
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		byte[] dataByteArr=String.valueOf(((java.lang.Boolean)o)).getBytes(idCharset);
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
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BOOLEAN_FOR_NULL;
			return DATA_LEN_TO_RETURN_FOR_NULL;
		}else{
			md.update(buff,((java.lang.Boolean)o).booleanValue()?1:0,1);
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BOOLEAN;;
			byte[] dataByteArr=String.valueOf((java.lang.Boolean)o).getBytes(idCharset);
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
		return COMPARE_AS_BOOLEAN;
	}
}
