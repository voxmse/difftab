package mse.difftab;

import java.security.MessageDigest;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public abstract class Hasher {
	protected static final int CHUNK_SIZE_CHARS = 32768; 
	protected static final int CHUNK_SIZE_BYTES = 131072; 
	
	private static final String HASHER_MAPPING_RESOURCE = "hasher/mapping.properties";
	private static Properties hasherMapping;
	
	public abstract void getHash(Object o,byte[] hash,int hashOffset)throws Exception;
	public abstract int getData(Object o,byte[] data,int dataOffset,int maxDataLength)throws Exception;
	public abstract int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength)throws Exception;
	
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

	protected static final byte[] longToByteArray(long value){
		byte[] h = new byte[8]; 
		h[0]=(byte)(value>>56);
		h[1]=(byte)(value>>48);
		h[2]=(byte)(value>>40);
		h[3]=(byte)(value>>32);
		h[4]=(byte)(value>>24);
		h[5]=(byte)(value>>16);
		h[6]=(byte)(value>>8);
		h[7]=(byte)value;
		return h;
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
		Hasher.timeFormat=timeFormat;
	}

	protected static void setHashMethod(String algo)throws Exception{
		Hasher.algo=algo;
		HASH_LENGTH=MessageDigest.getInstance(algo).getDigestLength();
		HASH_LENGTH_FULL=HASH_LENGTH+HASH_SUFFIX_LENGTH;
	}
	
	protected static final byte COMPARE_AS_BIG_DECIMAL = 	-128;
	protected static final byte COMPARE_AS_STRING = 		-120;
	protected static final byte COMPARE_AS_TIMESTAMP = 		-112;
	protected static final byte COMPARE_AS_RAW = 			-104;
	protected static final byte COMPARE_AS_OBJECT = 		-96;
	protected static final byte COMPARE_AS_ID = 			-88;
	protected static final byte COMPARE_AS_BOOLEAN = 		-80;
	protected static final byte COMPARE_AS_TIME = 			-72;
	protected static final byte COMPARE_AS_DUMMY = 			-64;
	protected static final byte COMPARE_AS_MAP = 			8;
	protected static final byte COMPARE_AS_SET = 			16;
	protected static final byte COMPARE_AS_LIST = 			24;
	protected static final byte COMPARE_AS_UNKNOWN =	 	120;
	
	protected static final byte COMPARE_AS_TYPELESS = 		0;

	protected static byte COMPARE_AS_BIG_DECIMAL_FOR_NULL =	-128;
	protected static byte COMPARE_AS_STRING_FOR_NULL = 		-120;
	protected static byte COMPARE_AS_TIMESTAMP_FOR_NULL =	-112;
	protected static byte COMPARE_AS_RAW_FOR_NULL = 		-104;
	protected static byte COMPARE_AS_OBJECT_FOR_NULL = 		-96;
	protected static byte COMPARE_AS_ID_FOR_NULL = 		-88;
	protected static byte COMPARE_AS_BOOLEAN_FOR_NULL = 	-80;
	protected static byte COMPARE_AS_TIME_FOR_NULL = 		-72;
	protected static byte COMPARE_AS_MAP_FOR_NULL = 		8;
	protected static byte COMPARE_AS_SET_FOR_NULL = 		16;
	protected static byte COMPARE_AS_LIST_FOR_NULL = 		24;
	
	protected static byte COMPARE_AS_BIG_DECIMAL_FOR_EMPTY =-128;
	protected static byte COMPARE_AS_STRING_FOR_EMPTY =		-120;
	protected static byte COMPARE_AS_TIMESTAMP_FOR_EMPTY =	-112;
	protected static byte COMPARE_AS_RAW_FOR_EMPTY = 		-104;
	protected static byte COMPARE_AS_OBJECT_FOR_EMPTY =		-96;
	protected static byte COMPARE_AS_ID_FOR_EMPTY = 		-88;
	protected static byte COMPARE_AS_BOOLEAN_FOR_EMPTY = 	-80;
	protected static byte COMPARE_AS_TIME_FOR_EMPTY = 		-72;
	protected static byte COMPARE_AS_MAP_FOR_EMPTY = 		8;
	protected static byte COMPARE_AS_SET_FOR_EMPTY = 		16;
	protected static byte COMPARE_AS_LIST_FOR_EMPTY = 		24;

	
	protected static final byte HASH_UNKNOWN = 				0;
	protected static final byte HASH_NULL = 				1;
	protected static final byte HASH_EMPTY =				2;
	
	protected static byte COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = COMPARE_AS_UNKNOWN + HASH_NULL; 					
	protected static byte COMPARE_AS_TYPELESS_FOR_NULL = COMPARE_AS_UNKNOWN + HASH_NULL; 					

	
	public static final byte DATA_LEN_TO_WRITE_FOR_NULL =			(byte)127;
	public static final byte DATA_LEN_TO_WRITE_FOR_NO_COLUMN =		(byte)126;
	public static final byte DATA_LEN_TO_WRITE_FOR_NO_SERIALIZER = 	(byte)125;
	
	public static final int DATA_LEN_VAL_MAX_FOR_1_BYTE =	120;
	public static final int DATA_LEN_VAL_MAX =	 			32767;

	public static final byte DATA_LEN_TO_RETURN_FOR_NULL =			-1;
	public static final byte DATA_LEN_TO_RETURN_FOR_NO_COLUMN =		-2;
	public static final byte DATA_LEN_TO_RETURN_FOR_NO_SERIALIZER = -3;

	
	public abstract byte getCompareAs();

	protected static MessageDigest getMessageDigestInstance()throws Exception{
		return MessageDigest.getInstance(algo);
	}
	
	private static String algo;
	protected MessageDigest md;
	protected static int HASH_LENGTH;
	protected static final int HASH_SUFFIX_LENGTH = 1;
	protected static int HASH_LENGTH_FULL;
	protected static java.nio.charset.Charset idCharset;
	protected static String dateFormat;
	protected static String timestampFormat;
	protected static String timeFormat;
	protected static String NESTED_ELEMENT_SEPARATOR;
	
	protected void free(Object o)throws Exception{
		md.reset();
	}
	
	public void setMessageDigest(MessageDigest md) {
		this.md = md;
	}
	
	protected void reset()throws Exception{
		md.reset();
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
	
	public static final void setParameters(boolean treatEmptyAsNull, boolean treatNoSuchColumnAsNull, boolean nullIsTypeless) {
		if(treatEmptyAsNull && treatNoSuchColumnAsNull && nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_ID_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_SET_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
		}else if(!treatEmptyAsNull && treatNoSuchColumnAsNull && nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_LIST;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
		}else if(treatEmptyAsNull && !treatNoSuchColumnAsNull && nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_ID_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_SET_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_NULL + COMPARE_AS_TYPELESS;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_UNKNOWN + COMPARE_AS_TYPELESS;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
		}else if(!treatEmptyAsNull && !treatNoSuchColumnAsNull && nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_LIST;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_UNKNOWN + COMPARE_AS_TYPELESS;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_TYPELESS;
		}else if(treatEmptyAsNull && treatNoSuchColumnAsNull && !nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_NULL + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_NULL + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_NULL + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_NULL + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_NULL + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_EMPTY = HASH_NULL + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_NULL + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_NULL + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_NULL + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_EMPTY = HASH_NULL + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_NULL + COMPARE_AS_LIST;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_LIST;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_NULL + COMPARE_AS_UNKNOWN;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_UNKNOWN;
		}else if(!treatEmptyAsNull && treatNoSuchColumnAsNull && !nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_LIST;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_LIST;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_NULL + COMPARE_AS_UNKNOWN;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_UNKNOWN;
		}else if(treatEmptyAsNull && !treatNoSuchColumnAsNull && !nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_NULL + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_NULL + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_NULL + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_NULL + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_NULL + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_EMPTY = HASH_NULL + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_NULL + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_NULL + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_NULL + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_EMPTY = HASH_NULL + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_NULL + COMPARE_AS_LIST;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_LIST;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_UNKNOWN + COMPARE_AS_UNKNOWN;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_UNKNOWN;
		}else if(!treatEmptyAsNull && !treatNoSuchColumnAsNull && !nullIsTypeless) {
			COMPARE_AS_BIG_DECIMAL_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_EMPTY = HASH_EMPTY + COMPARE_AS_LIST;
			
			COMPARE_AS_BIG_DECIMAL_FOR_NULL = HASH_NULL + COMPARE_AS_BIG_DECIMAL;
			COMPARE_AS_STRING_FOR_NULL = HASH_NULL + COMPARE_AS_STRING;
			COMPARE_AS_TIMESTAMP_FOR_NULL =	HASH_NULL + COMPARE_AS_TIMESTAMP;
			COMPARE_AS_RAW_FOR_NULL = HASH_NULL + COMPARE_AS_RAW;
			COMPARE_AS_OBJECT_FOR_NULL = HASH_NULL + COMPARE_AS_OBJECT;
			COMPARE_AS_ID_FOR_NULL = HASH_NULL + COMPARE_AS_ID;
			COMPARE_AS_BOOLEAN_FOR_NULL = HASH_NULL + COMPARE_AS_BOOLEAN;
			COMPARE_AS_TIME_FOR_NULL = HASH_NULL + COMPARE_AS_TIME;
			COMPARE_AS_MAP_FOR_NULL = HASH_NULL + COMPARE_AS_MAP;
			COMPARE_AS_SET_FOR_NULL = HASH_NULL + COMPARE_AS_SET;
			COMPARE_AS_LIST_FOR_NULL = HASH_NULL + COMPARE_AS_LIST;

			COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN = HASH_UNKNOWN + COMPARE_AS_UNKNOWN;
			COMPARE_AS_TYPELESS_FOR_NULL = HASH_NULL + COMPARE_AS_UNKNOWN;
		}
	}
	
	protected TabInfo ti;
	protected String colName;
	
	public void setInfo(TabInfo ti,String colName) throws ConfigValidationException {
		this.ti=ti;
		this.colName=colName;
	}
	
	public void setInfoInternal(Pattern[] excludedColumns, String colName){
	}
	
	private static Map<String, Class<?>> cache = new ConcurrentHashMap<String, Class<?>>();

	protected static Class<?> getHasherClass(String className) throws RuntimeException {
		try{
			Class<?> c = Class.forName(className);
			return getHasherClass(c);
		}catch(ClassNotFoundException e) {
			String ss = hasherMapping.getProperty(className);
			if(ss != null) {
				try{
					return Class.forName(ss);
				}catch(Exception ee){
					throw new RuntimeException("Can not find the hasher class \"" + ss + "\" for the \"" + className + "\" Jdbc column value class name : " + ee.getMessage());
				}
			}else {
				throw new RuntimeException("Can not determine hasher class for the \"" + className + "\" Jdbc column value class name");
			}
		}catch(Exception unexpectedException){
			throw new RuntimeException("Can not determine hasher class for the \"" + className + "\" Jdbc column value class name : " + unexpectedException.getMessage());
		}
	}
	
	protected static Class<?> getHasherClass(Class<?> c) throws RuntimeException {
		String s = c.getName();
		Class<?> h = cache.get(s);
		if(h != null) {
			return h;
		}else{
			h=cache.get(c.getSuperclass().getName());
			if(h != null && !"java.lang.Object".equals(h)){
				cache.put(s, h);				
				return h;
			}else{
				for(Class<?> i : c.getInterfaces()) {
					h = cache.get(i.getName());
					if(h != null) {
						cache.put(s, h);
						return h;
					}
				}
			}
		}
		// not found in the cache
		String ss = hasherMapping.getProperty(s);
		Class<?> cc = null;
		if(ss != null){
			try {
				cc = Class.forName(ss);
			}catch(Exception e){
				throw new RuntimeException("Can not get hasher", e);
			}
			cache.put(s, cc);
			return cc;
		}else{
			ss=c.getSuperclass().getName();
			if(!"java.lang.Object".equals(ss) && hasherMapping.containsKey(ss)) {
				ss=hasherMapping.getProperty(ss);
				try {
					cc = Class.forName(ss);
				}catch(Exception e){
					throw new RuntimeException("Can not get hasher", e);
				}
				cache.put(s,cc);				
				return cc;
			}else{
				for(Class<?> i : c.getInterfaces()) {
					ss = hasherMapping.getProperty(i.getName());
					if(ss != null) {
						try{
							cc = Class.forName(ss);
						}catch(Exception e){
							throw new RuntimeException("Can not get hasher", e);
						}
						cache.put(s,cc);				
						return cc;
					}
				}
			}
		}
		
		throw new RuntimeException("Can not get hasher for " + s);
	}
	
	static {
		try{
			hasherMapping = DiffTab.getProperties(HASHER_MAPPING_RESOURCE);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}