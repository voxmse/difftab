package mse.difftab.hasher;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import mse.difftab.Hasher;
import mse.difftab.TabInfo;

public class CASSANDRA_TUPLE extends Hasher {
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o == null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_LIST_FOR_NULL;
		}else{
			@SuppressWarnings("unchecked")
			List<Object> lst = (List<Object>)o;
			if(lst.size() == 0) {
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_LIST_FOR_EMPTY;
			}else{
				byte[] hashes = new byte[lst.size() * HASH_LENGTH_FULL];
				for(int i=0; i<lst.size(); i++){
					Object val = lst.get(i);
					if(val == null){
						Arrays.fill(hashes, i * HASH_LENGTH_FULL, (i + 1) * HASH_LENGTH_FULL, (byte)0);
						hashes[(i + 1) * HASH_LENGTH_FULL] = COMPARE_AS_TYPELESS_FOR_NULL;	
					}else{
						Hasher h = (Hasher) Hasher.getHasherClass(val.getClass()).newInstance();
						h.setMessageDigest(this.md);
						h.setInfoInternal(disabledColumns, colName + NESTED_ELEMENT_SEPARATOR + String.valueOf(i));
						h.getHash(val, hashes, i * HASH_LENGTH_FULL);
					}
				}
				md.update(hashes);
				md.digest(hash,hashOffset+1,HASH_LENGTH);
				hash[hashOffset] = COMPARE_AS_MAP;
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
