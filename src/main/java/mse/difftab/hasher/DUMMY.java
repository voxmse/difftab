package mse.difftab.hasher;

import mse.difftab.Hasher;


public class DUMMY extends Hasher {
	@Override
	protected void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
	}

	@Override
	protected int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		return 0;
	}

	@Override
	protected int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength) throws Exception {
		return 0;
	}
}
