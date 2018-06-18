package mse.difftab.hasher;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import mse.difftab.Hasher;

public class LONG extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BIG_DECIMAL_FOR_NULL;
		}else{
			md.update(BigDecimal.valueOf((java.lang.Long)o).stripTrailingZeros().toPlainString().getBytes(StandardCharsets.US_ASCII));
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BIG_DECIMAL;
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		final byte[] val=BigDecimal.valueOf((java.lang.Long)o).stripTrailingZeros().toPlainString().getBytes(idCharset);
		if(val.length>=maxDataLength){
			for(int i=0;i<maxDataLength;i++)data[dataOffset+(maxDataLength<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2)+i]=val[i];			
			return maxDataLength;
		}else{
			for(int i=0;i<val.length;i++)data[dataOffset+(val.length<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2)+i]=val[i];			
			return val.length;
		}
	}

	@Override
	public int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BIG_DECIMAL_FOR_NULL;
			return DATA_LEN_TO_RETURN_FOR_NULL;
		}else{
			final String strVal=BigDecimal.valueOf((java.lang.Long)o).stripTrailingZeros().toPlainString();
			final byte[] valData=strVal.getBytes(idCharset);
			md.update(strVal.getBytes(StandardCharsets.US_ASCII));
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_BIG_DECIMAL;
			if(valData.length>=maxDataLength){
				for(int i=0;i<maxDataLength;i++)data[dataOffset+(maxDataLength<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2)+i]=valData[i];				
				return maxDataLength;
			}else{
				for(int i=0;i<valData.length;i++)data[dataOffset+(valData.length<=DATA_LEN_VAL_MAX_FOR_1_BYTE?1:2)+i]=valData[i];				
				return valData.length;
			}
		}
	}
	
	public byte getCompareAs() {
		return COMPARE_AS_BIG_DECIMAL;
	}
}