/**
 * 
 */
package mse.difftab.hasher;

import mse.difftab.Hasher;

/**
 * @author m
 *
 */
public class SQLXML extends Hasher {

	/* (non-Javadoc)
	 * @see mse.difftab.Hasher#getHash(java.lang.Object, byte[], int)
	 */
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see mse.difftab.Hasher#getData(java.lang.Object, byte[], int, int)
	 */
	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see mse.difftab.Hasher#getHashAndData(java.lang.Object, byte[], int, byte[], int, int)
	 */
	@Override
	public int getHashAndData(Object o, byte[] hash, int hashOffset, byte[] data, int dataOffset, int maxDataLength)
			throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see mse.difftab.Hasher#getCompareAs()
	 */
	@Override
	public byte getCompareAs() {
		// TODO Auto-generated method stub
		return 0;
	}

}
