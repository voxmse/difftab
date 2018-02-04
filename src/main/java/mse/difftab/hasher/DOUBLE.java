package mse.difftab.hasher;


import java.nio.charset.StandardCharsets;

import mse.difftab.Hasher;

public class DOUBLE extends Hasher {
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}
	
	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			md.update((new java.math.BigDecimal(((Double)o).toString())).stripTrailingZeros().toPlainString().getBytes(StandardCharsets.US_ASCII));
			md.digest(hash,hashOffset,HASH_LENGTH);
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		if(o==null) return -1;
		final byte[] val=((Double)o).toString().getBytes(idCharset);
		if(val.length>=maxDataLength){
			for(int i=0;i<maxDataLength;i++)data[dataOffset+(maxDataLength<127?1:2)+i]=val[i];
			return maxDataLength;
		}else{
			for(int i=0;i<val.length;i++)data[dataOffset+(val.length<127?1:2)+i]=val[i];
			return val.length;
		}
	}

	@Override
	protected int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
			return -1;
		}else{
			md.update((new java.math.BigDecimal(((Double)o).toString())).stripTrailingZeros().toPlainString().getBytes(StandardCharsets.US_ASCII));
			md.digest(hash,hashOffset,HASH_LENGTH);
			final byte[] valData=((Double)o).toString().getBytes(idCharset);
			if(valData.length>=maxDataLength){
				for(int i=0;i<maxDataLength;i++)data[dataOffset+(maxDataLength<127?1:2)+i]=valData[i];
				return maxDataLength;
			}else{
				for(int i=0;i<valData.length;i++)data[dataOffset+(valData.length<127?1:2)+i]=valData[i];
				return valData.length;
			}
		}
	}
}
