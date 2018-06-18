package mse.difftab.hasher;

import mse.difftab.Hasher;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class TIME extends Hasher {
	private DateTimeFormatter f=DateTimeFormatter.ofPattern(timeFormat).withZone(ZoneOffset.UTC);;
	private byte[] buff = new byte[12];
	
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_TIME_FOR_NULL;
		}else{
			longToByteArray(((java.sql.Time)o).getTime(),buff,0);
			md.update(buff);
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_TIME;
		}
	}

	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return DATA_LEN_TO_RETURN_FOR_NULL;
		final byte[] dataByteArr=((java.sql.Time)o).toLocalTime().format(f).getBytes(idCharset);
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
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_TIME_FOR_NULL;
			return DATA_LEN_TO_RETURN_FOR_NULL;
		}else{
			longToByteArray(((java.sql.Time)o).getTime(),buff,0);
			md.update(buff);
			md.digest(hash, hashOffset, HASH_LENGTH);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_TIME;
			final byte[] dataByteArr=((java.sql.Time)o).toLocalTime().format(f).getBytes(idCharset);
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
		return COMPARE_AS_TIME;
	}
}
