package mse.difftab.hasher;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import mse.difftab.Hasher;
import mse.difftab.TabInfo;
import mse.difftab.ColInfo;

public class MAP_AS_COLUMNSET extends Hasher {
	@SuppressWarnings("unchecked")
	@Override
	public void getHash(Object o, byte[] hash, int hashOffset) throws Exception {
		if(o == null){
			Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
			hash[hashOffset + HASH_LENGTH] = COMPARE_AS_MAP_FOR_NULL;					
		}else{
			if(ColInfo.OVERALL_COLUMN_NAME.equals(colName)){
				// for the rest of columns
				Object[] pairs = ((Map<String,Object>)o).entrySet().stream().filter(k -> !checkIfEnlisted(k.getKey()) && !checkIfDisabled(k.getKey())).sorted(Map.Entry.comparingByKey()).toArray();
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
							h.setInfoInternal(disabledColumns, colName + NESTED_ELEMENT_SEPARATOR + ((Map.Entry<String,Object>)pairs[i]).getKey());
							h.getHash(val, hashes, ((i * 2) + 1) * HASH_LENGTH_FULL);
						}
					}
					md.update(hashes);
					md.digest(hash,hashOffset+1,HASH_LENGTH);
					hash[hashOffset] = COMPARE_AS_MAP;						
				}
			}else if(colName != null){
				// for a given column
				if(((Map<String,Object>)o).containsKey(colName)){
					Object colObject = ((Map<String,Object>)o).get(colName);
					if(colObject == null) {
						Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
						hash[hashOffset + HASH_LENGTH] = COMPARE_AS_TYPELESS_FOR_NULL;						
					}else{
						Hasher h = (Hasher) Hasher.getHasherClass(colObject.getClass()).newInstance();
						h.setMessageDigest(this.md);
						h.setInfoInternal(disabledColumns, colName);
						h.getHash(colObject, hash, hashOffset);
					}
				}else{
					Arrays.fill(hash, hashOffset, hashOffset + HASH_LENGTH, (byte)0);
					hash[hashOffset + HASH_LENGTH] = COMPARE_AS_TYPELESS_FOR_NO_SUCH_COLUMN;
				}
			}else{
				throw new RuntimeException("Map hasher : column name is not defined");
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public int getData(Object o, byte[] data, int dataOffset, int maxDataLength)throws Exception {
		if(!ColInfo.OVERALL_COLUMN_NAME.equals(colName)){
			if(((Map<String,Object>)o).containsKey(colName)){
				Object colObject = ((Map<String,Object>)o).get(colName);
				if(colObject == null) {
					return DATA_LEN_TO_RETURN_FOR_NULL;
				}else{
					Hasher h = (Hasher) Hasher.getHasherClass(colObject.getClass()).newInstance();
					return h.getData(colObject, data, dataOffset, maxDataLength);
				}
			}else {
				return DATA_LEN_TO_RETURN_FOR_NO_COLUMN;
			}
		}else{
			return DATA_LEN_TO_RETURN_FOR_NO_SERIALIZER;
		}
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
			enlistedColumns = ti.columns.values().stream().map(ci -> ci.dbName).toArray(String[]::new);
		}
	}
	
	@Override
	public void setInfoInternal(Pattern[] excludedColumns, String colName){
		this.colName = colName;
		this.disabledColumns = excludedColumns;
	}
	
	private Pattern[] disabledColumns;
	private String[] enlistedColumns;
	private STRING sh = new STRING();

	private boolean checkIfDisabled(String colName) {
		for(int i=0; i<disabledColumns.length; i++)
			if(disabledColumns[i].matcher(colName).matches())
				return true;
		return false;
	}
	
	private boolean checkIfEnlisted(String colName) {
		for(int i=0; i<enlistedColumns.length; i++)
			if(enlistedColumns[i].equals(colName))
				return true;
		return false;
	}
	
}
