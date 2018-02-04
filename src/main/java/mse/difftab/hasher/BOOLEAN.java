package mse.difftab.hasher;


import mse.difftab.Hasher;


public class BOOLEAN extends Hasher {
	byte[] buff;
	
	public BOOLEAN(){
		super();
		buff=new byte[]{0x0,0x1};
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
			md.update(buff,((java.lang.Boolean)o).booleanValue()?1:0,1);
			md.digest(hash,hashOffset,HASH_LENGTH);
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return -1;
		byte[] dataByteArr=String.valueOf(((java.lang.Boolean)o)).getBytes(idCharset);
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
			md.update(buff,((java.lang.Boolean)o).booleanValue()?1:0,1);
			md.digest(hash,hashOffset,HASH_LENGTH);
			byte[] dataByteArr=String.valueOf((java.lang.Boolean)o).getBytes(idCharset);
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
