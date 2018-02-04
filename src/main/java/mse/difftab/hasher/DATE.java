package mse.difftab.hasher;

import mse.difftab.Hasher;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DATE extends Hasher {
	private DateTimeFormatter f=DateTimeFormatter.ofPattern(dateFormat).withZone(ZoneOffset.UTC);
	private byte[] buff = new byte[12];
	
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}
	
	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			longToByteArray(((java.sql.Date)o).getTime(),buff,0);
			md.update(buff);
			md.digest(hash,hashOffset,HASH_LENGTH);
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return -1;
		final byte[] dataByteArr=((java.sql.Date)o).toLocalDate().format(f).getBytes(idCharset);
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
			longToByteArray(((java.sql.Date)o).getTime(),buff,0);
			md.update(buff);
			md.digest(hash,hashOffset,HASH_LENGTH);
			final byte[] dataByteArr=((java.sql.Date)o).toLocalDate().format(f).getBytes(idCharset);
			if(dataByteArr.length>=maxDataLength){
				System.arraycopy(dataByteArr,0,data,dataOffset+(maxDataLength<127?1:2),maxDataLength);
				return maxDataLength;
			}else{
				System.arraycopy(dataByteArr,0,data,dataOffset+(dataByteArr.length<127?1:2),dataByteArr.length);
				return dataByteArr.length;
			}
		}
	}
}
