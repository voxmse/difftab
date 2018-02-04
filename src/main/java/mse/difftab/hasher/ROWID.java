package mse.difftab.hasher;

import mse.difftab.Hasher;

import java.sql.RowId;

public class ROWID extends Hasher {
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}

	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			final byte[] value=((RowId)o).getBytes();
			if(value.length>0){
				md.update(value);
				md.digest(hash,hashOffset,HASH_LENGTH);
			}else{
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset,int maxDataLength)throws Exception {
		if(o==null) return -1;
		final byte[] dataByteArr=((RowId)o).toString().getBytes(idCharset);
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
			byte[] value=((RowId)o).getBytes();
			if(value.length>0){
				md.update(value);
				md.digest(hash,hashOffset,HASH_LENGTH);
			}else{
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}
			value=null;
			final byte[] dataByteArr=((RowId)o).toString().getBytes(idCharset);
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
