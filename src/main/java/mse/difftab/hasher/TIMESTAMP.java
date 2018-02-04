package mse.difftab.hasher;

import mse.difftab.Hasher;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TIMESTAMP extends Hasher {
	private	DateTimeFormatter f;
	private byte[] buff = new byte[12];
	
	public TIMESTAMP(){
		super();
		f=DateTimeFormatter.ofPattern(dateFormat).withZone(ZoneOffset.UTC);
	}
	
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}

	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			longToByteArray(((java.sql.Timestamp)o).getTime(),buff,0);
			intToByteArray(((java.sql.Timestamp)o).getNanos(),buff,8);
			md.update(buff);
			md.digest(hash,hashOffset,HASH_LENGTH);
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return -1;
		final byte[] dataByteArr=((java.sql.Timestamp)o).toLocalDateTime().format(f).getBytes(idCharset);
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
			longToByteArray(((java.sql.Timestamp)o).getTime(),buff,0);
			intToByteArray(((java.sql.Timestamp)o).getNanos(),buff,8);
			md.update(buff);
			md.digest(hash,hashOffset,HASH_LENGTH);
			final byte[] dataByteArr=((java.sql.Timestamp)o).toLocalDateTime().format(f).getBytes(idCharset);
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
