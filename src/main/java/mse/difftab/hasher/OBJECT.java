package mse.difftab.hasher;

import java.nio.charset.StandardCharsets;

import mse.difftab.Hasher;

public class OBJECT extends Hasher {
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}

	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			String dataStr=o.toString();
			if(dataStr.isEmpty()){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else{
				md.update(dataStr.getBytes(StandardCharsets.UTF_8));
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(o==null) return -1;
		final byte[] dataByteArr=o.toString().getBytes(idCharset);
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
			String dataStr=o.toString();
			if(dataStr.isEmpty()){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else{
				md.update(dataStr.getBytes(StandardCharsets.UTF_8));
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
			dataStr=null;
			final byte[] dataByteArr=o.toString().getBytes(idCharset);
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
