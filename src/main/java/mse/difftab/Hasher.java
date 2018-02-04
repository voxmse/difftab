package mse.difftab;

import java.security.MessageDigest;

public abstract class Hasher {
	protected static final int CHUNK_SIZE_CHAR = 32768; 
	protected static final int CHUNK_SIZE_BYTE = 131072; 
	
	protected abstract void getHash(Object o,byte[] hash,int hashOffset)throws Exception;
	protected abstract int getData(Object o,byte[] data,int dataOffset,int maxDataLength)throws Exception;
	protected abstract int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength)throws Exception;
	
	protected static final void longToByteArray(long value,byte[] h,int offset){
		h[offset++]=(byte)(value>>56);
		h[offset++]=(byte)(value>>48);
		h[offset++]=(byte)(value>>40);
		h[offset++]=(byte)(value>>32);
		h[offset++]=(byte)(value>>24);
		h[offset++]=(byte)(value>>16);
		h[offset++]=(byte)(value>>8);
		h[offset]=(byte)value;
	}
	
	protected static final void intToByteArray(long value,byte[] h,int offset){
		h[offset++]=(byte)(value>>24);
		h[offset++]=(byte)(value>>16);
		h[offset++]=(byte)(value>>8);
		h[offset]=(byte)value;
	}

	protected static final void shortToByteArray(long value,byte[] h,int offset){
		h[offset++]=(byte)(value>>8);
		h[offset]=(byte)value;
	}

	protected static void setCommonValues(java.nio.charset.Charset idCharset,String dateFormat,String timestampFormat,String timeFormat){
		Hasher.idCharset=idCharset;
		Hasher.dateFormat=dateFormat;
		Hasher.timestampFormat=timestampFormat;
		dummy = 0;
	}

	protected static void setHashMethod(String algo)throws Exception{
		Hasher.algo=algo;
		HASH_LENGTH=MessageDigest.getInstance(algo).getDigestLength();
		HASH_NULL=new byte[HASH_LENGTH];
		HASH_NULL[0]=(byte)0x80;
		HASH_EMPTY=new byte[HASH_LENGTH];
		HASH_EMPTY[0]=(byte)0x81;
		HASH_INITIAL=new byte[HASH_LENGTH];
		HASH_INITIAL[0]=(byte)0x82;
		dummy = 0;
	}

	protected static MessageDigest getMessageDigestInstance()throws Exception{
		return MessageDigest.getInstance(algo);
	}
	
	private static String algo;
	protected MessageDigest md;
	protected static int HASH_LENGTH;
	protected static java.nio.charset.Charset idCharset;
	protected static String dateFormat;
	protected static String timestampFormat;
	protected static String timeFormat;
	protected static final int NULL_VALUE=-1;
	protected static byte[] HASH_NULL;
	protected static byte[] HASH_EMPTY;
	protected static byte[] HASH_INITIAL;
	@SuppressWarnings("unused")
	private static volatile int dummy;
	
	protected boolean getDataIsSupported(){
		return false;
	}
	
	protected void free(Object o)throws Exception{
		md.reset();
	}
	
	protected void setMessageDigest(MessageDigest md) {
		this.md = md;
	}
	
	protected void reset()throws Exception{
		md.reset();
	}

	protected static final int getHashBacket(byte[] hash,int buckets){
		return buckets==1?0:((hash[0]&0xff>>1<<24)+(hash[1]&0xff<<16)+(hash[2]&0xff<<8)+(hash[3]&0xff))%buckets;
	}
	
	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes,int offset, int len) {
	    char[] hexChars = new char[len * 2];
	    for ( int j = 0; j < len; j++ ) {
	        int v = bytes[offset+j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
}