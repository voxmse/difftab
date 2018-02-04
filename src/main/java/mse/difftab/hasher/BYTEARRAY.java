package mse.difftab.hasher;

import mse.difftab.Hasher;

public class BYTEARRAY extends Hasher {
	@Override
	protected boolean getDataIsSupported(){
		return true;
	}
	
	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o==null){
			System.arraycopy(HASH_NULL,0,hash,hashOffset,HASH_LENGTH);
		}else{
			final byte[] byteArr=(byte[])o;
			if(byteArr.length == 0){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else if(byteArr.length <= CHUNK_SIZE_BYTE){
				md.update(byteArr);
				md.digest(hash,hashOffset,HASH_LENGTH);
			}else{
				for(int chunkBoundaryLeft=0;chunkBoundaryLeft<byteArr.length;) {					
					md.update(byteArr,chunkBoundaryLeft,chunkBoundaryLeft+Math.min(byteArr.length-chunkBoundaryLeft,CHUNK_SIZE_CHAR));
					chunkBoundaryLeft += CHUNK_SIZE_CHAR;
				}
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
		}
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(o==null) return -1;
		byte[] dataByteArr = Hasher.bytesToHex((byte[])o,0,Math.min(maxDataLength,((byte[])o).length/2)).getBytes(idCharset);
		if(dataByteArr.length*2>=maxDataLength){
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
			byte[] byteArr=(byte[])o;
			if(byteArr.length == 0){
				System.arraycopy(HASH_EMPTY,0,hash,hashOffset,HASH_LENGTH);
			}else if(byteArr.length <= CHUNK_SIZE_BYTE){
				md.update(byteArr);
				md.digest(hash,hashOffset,HASH_LENGTH);
			}else{
				for(int chunkBoundaryLeft=0;chunkBoundaryLeft<byteArr.length;) {					
					md.update(byteArr,chunkBoundaryLeft,chunkBoundaryLeft+Math.min(byteArr.length-chunkBoundaryLeft,CHUNK_SIZE_CHAR));
					chunkBoundaryLeft += CHUNK_SIZE_CHAR;
				}
				md.digest(hash,hashOffset,HASH_LENGTH);
			}
			byteArr=null;
			byte[] dataByteArr = Hasher.bytesToHex((byte[])o,0,Math.min(maxDataLength,((byte[])o).length/2)).getBytes(idCharset);
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
