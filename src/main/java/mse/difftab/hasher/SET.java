package mse.difftab.hasher;

import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import mse.difftab.Hasher;
import mse.difftab.TabInfo;

public class SET extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o == null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_SET_FOR_NULL;
		}else{
			@SuppressWarnings("unchecked")
			Set<Object> set = (Set<Object>)o;
			if(set.size() == 0) {
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_SET_FOR_EMPTY;
			}else{
				byte[][] hashes = new byte[set.size()][HASH_LENGTH_FULL];
				int i = 0;
				for(Object val : set){
					if(val == null){
						Arrays.fill(hashes[i], 0, HASH_LENGTH_FULL, (byte)0);
						hashes[i++][HASH_LENGTH_FULL] = COMPARE_AS_TYPELESS_FOR_NULL;	
					}else{
						Hasher h = (Hasher) Hasher.getHasherClass(val.getClass()).newInstance();
						h.setMessageDigest(this.md);
						h.setInfoInternal(disabledColumns, colName + NESTED_ELEMENT_SEPARATOR);
						h.getHash(val, hashes[i], 0);
					}
				}
				
				java.util.Arrays.sort(hashes, new java.util.Comparator<byte[]>() {
				    public int compare(byte[] a, byte[] b) {
				        for(int i=0; i<HASH_LENGTH_FULL; i++) {
				        	if(a[i]<b[i])return -1;
				        	if(a[i]>b[i])return 1;
				        }
				    	return 0;
				    }
				});
				
				for(i=0; i<set.size(); i++)	md.update(hashes[i]);
				md.digest(hash,hashOffset+1,HASH_LENGTH);
				hash[hashOffset] = COMPARE_AS_SET;
			}
		}
	}
	
	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		return DATA_LEN_TO_RETURN_FOR_NO_SERIALIZER;
	}

	@Override
	public int getHashAndData(Object o,byte[] hash,int hashOffset,byte[] data,int dataOffset,int maxDataLength) throws Exception {
		getHash(o, hash, hashOffset);
		return getData(o, data, dataOffset, maxDataLength);
	}
	
	@Override
	public byte getCompareAs() {
		return COMPARE_AS_LIST;
	}
	
	@Override
	public void setInfoInternal(Pattern[] excludedColumns, String colName){
		this.colName = colName;
		this.disabledColumns = excludedColumns;
	}

	@Override
	public void setInfo(TabInfo ti,String colName) throws mse.difftab.ConfigValidationException {
		super.setInfo(ti, colName);
		if(ti != null){
			try {
				disabledColumns = ti.columns.values().stream().filter(ci -> ci.hashIdx<=0).map(ci -> Pattern.compile(ci.dbName)).toArray(Pattern[]::new);
			}catch(PatternSyntaxException e){
				throw new mse.difftab.ConfigValidationException("Wrong REGEX expression for the column \""+colName+"\":");
			}
		}
	}

	private Pattern[] disabledColumns;
}
