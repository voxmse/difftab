package mse.difftab.hasher;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import mse.difftab.Hasher;
import mse.difftab.TabInfo;

public class MAP extends Hasher {
	@SuppressWarnings("unchecked")
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(ti!=null){
			if(o == null){
				Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
				hash[hashOffset + HASH_LENGTH] = COMPARE_AS_MAP_FOR_NULL;					
			}else{
				Object[] pairs = ((Map<String,Object>)o).entrySet().stream().filter(k -> !checkIfDisabled(k.getKey())).sorted(Map.Entry.comparingByKey()).toArray();
				if(pairs.length == 0) {
					Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
					hash[hashOffset + HASH_LENGTH] = COMPARE_AS_MAP_FOR_EMPTY;
				}else{
					byte[] hashes = new byte[pairs.length * 2 * HASH_LENGTH_FULL];
					sh.setMessageDigest(this.md);
					for(int i=0; i<pairs.length; i++){
						sh.getHash(((Map.Entry<String,Object>)pairs[i]).getKey(), hashes, i * 2 * HASH_LENGTH_FULL);
						Object val = ((Map.Entry<String,Object>)pairs[i]).getValue();
						if(val == null){
							Arrays.fill(hashes, ((i * 2) + 1) * HASH_LENGTH_FULL, ((i * 2) + 1) * HASH_LENGTH_FULL + HASH_LENGTH, (byte)0);
							hashes[((i * 2) + 1) * HASH_LENGTH_FULL + HASH_LENGTH] = COMPARE_AS_TYPELESS_FOR_NULL;	
						}else{
							Hasher h = (Hasher) Hasher.getHasherClass(val.getClass()).newInstance();
							h.setMessageDigest(this.md);
							h.setInfo(null, colName + NESTED_ELEMENT_SEPARATOR + ((Map.Entry<String,Object>)pairs[i]).getKey());
							h.getHash(val, hashes, ((i * 2) + 1) * HASH_LENGTH_FULL);
						}
					}
					md.update(hashes);
					md.digest(hash,hashOffset+1,HASH_LENGTH);
					hash[hashOffset] = COMPARE_AS_MAP;
				}
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
		return COMPARE_AS_MAP;
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
	
	@Override
	public void setInfoInternal(Pattern[] excludedColumns, String colName){
		this.colName = colName;
		this.disabledColumns = excludedColumns;
	}
	
	private Pattern[] disabledColumns;
	private STRING sh = new STRING();

	private boolean checkIfDisabled(String colName) {
		for(int i=0; i<disabledColumns.length; i++)
			if(disabledColumns[i].matcher(colName).matches())
				return true;
		return false;
	}
}
