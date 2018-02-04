package mse.difftab;

import java.util.Map;

import mse.difftab.prepared.Prepared;

public class TabInfo {
	public String schema;
	public String dbName;
	public String fullName;
	public String alias;
	public Map<String,ColInfo> columns;
	public long rows;
	public String query;
	public boolean groupByKey;
	public int confSrcTabIdx;
	public Prepared.Table prepared;
}
