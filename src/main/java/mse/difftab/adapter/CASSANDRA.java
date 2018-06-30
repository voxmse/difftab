package mse.difftab.adapter;

import mse.difftab.ColInfo;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.sql.ResultSet;

import mse.difftab.Adapter;
import mse.difftab.TabInfo;

public class CASSANDRA implements Adapter {
	private static final String CURRENT_KEYSPACE = "CURRENT_KEYSPACE";

	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

//		if(schemaFilter == null){
//			schemaFilter = getCurrentKeySpace(conn);
//		}
		String query="select keyspace_name,table_name from system_schema.tables";
		try{
			rs = st.executeQuery(query);
			while(rs.next()){
				@SuppressWarnings("unchecked")
				Map<String,Object> rowData = (Map<String,Object>)rs.getObject(1);
				if((schemaFilter == null || (rowData.get("keyspace_name") != null && ((String)rowData.get("keyspace_name")).matches(schemaFilter))) && (nameFilter == null || (rowData.get("table_name") != null && ((String)rowData.get("table_name")).matches(nameFilter)))) {
					ti = new TabInfo();
					ti.dbName = (String)rowData.get("table_name");
					ti.fullName = (String)rowData.get("keyspace_name") + "." + (String)rowData.get("table_name");
					ti.schema = (String)rowData.get("keyspace_name");
					ti.rows = -1;
					tabs.add(ti);
				}
			}
			rs.close();
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return tabs;
	}

	@SuppressWarnings("unchecked")
	private List<ColInfo> getPK(Connection conn,String schema,String table)throws Exception {
		List<ColInfo>cols=new ArrayList<ColInfo>();
		Statement st = null;
		ResultSet rs = null;
		Map<String,Object> rowData;
		try{
			st = conn.createStatement();
			rs = st.executeQuery("SELECT keyspace_name,table_name,column_name,kind FROM system_schema.columns"); 
			while(rs.next()){
				rowData = (Map<String,Object>)rs.getObject(1);
				if(rowData.get("keyspace_name") != null && schema.equals(rowData.get("keyspace_name")) && rowData.get("table_name") != null && table.equals(rowData.get("table_name")) && (rowData.get("kind") != null && ("partition_key".equals(rowData.get("kind")) || "clustering".equals(rowData.get("kind")))) && !cols.contains(rowData.get("column_name"))) {
					ColInfo ci = new ColInfo();
					ci.colIdx = cols.size() + 1;
					ci.dbName = (String)rowData.get("column_name");
					ci.fullName = ci.dbName;
					ci.alias = ci.dbName.toUpperCase();
					ci.hashIdx = 1;
					ci.keyIdx = 1;
					ci.jdbcClassName = "MAP_AS_COLUMNSET";
					ci.confSrcTabColIdx = -1;
					cols.add(ci);
				}
			}
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return cols;
	}

	private List<ColInfo> getROWID(Connection conn,String schema,String table)throws Exception {
		return new ArrayList<ColInfo>();	
	}

	@Override
	public List<ColInfo> getPK(Connection conn,String schema,String table,boolean isRowidPreffered)throws Exception{
		List<ColInfo> cols;
		if(isRowidPreffered){
			cols=getROWID(conn,schema,table);
			if(cols.size()==0){
				cols=getPK(conn,schema,table);
			}
		}else{
			cols= getPK(conn,schema,table);
			if(cols.size()==0){
				cols=getROWID(conn,schema,table);
			}
		}
		return cols;
	}

	@Override
	public List<ColInfo> getColumns(Connection conn,String schema,String table,String nameFilter)throws Exception {
		ArrayList<ColInfo>cols=new ArrayList<ColInfo>(); 
		ColInfo ci=new ColInfo();
		ci.colIdx = 1;
		ci.dbName=(nameFilter==null?ColInfo.OVERALL_COLUMN_NAME:nameFilter);
		ci.fullName = ci.dbName;
		ci.alias = ci.dbName.toUpperCase();
		ci.hashIdx = 1;
		ci.keyIdx = 0;
		ci.jdbcClassName = MAP_AS_COLUMNSET;
		ci.confSrcTabColIdx = -1;
		cols.add(ci);
		return cols;
	}

	@Override
	public boolean ColumnSetAndDataTypesAreFixed() {
		return false;
	}
	
	@Override
	public String getQuery(Connection conn, String schema, String table, List<ColInfo> columns){
		if(columns==null || columns.isEmpty() || columns.stream().anyMatch(ci -> ColInfo.OVERALL_COLUMN_NAME.equals(ci.alias) && (ci.hashIdx>0 || ci.keyIdx>0))){
			return "SELECT * FROM "+(schema==null?"":(schema+"."))+table;
		}else{
			return "SELECT "+columns.stream().map(ci -> ci.fullName).collect(Collectors.joining(","))+" FROM "+(schema==null?"":(schema+"."))+table;
		}
	}
	
	@Override
	public List<ColInfo> getColumns(Connection conn,String query)throws Exception{
		List<ColInfo> columns = new ArrayList<ColInfo>();
		ColInfo col = new ColInfo();
		col.colIdx = 1;
		col.dbName = ColInfo.OVERALL_COLUMN_NAME;
		col.fullName = col.dbName;
		col.alias = col.dbName.toUpperCase();
		col.jdbcClassName = MAP_AS_COLUMNSET;
		col.hashIdx = 1;
		col.keyIdx = 0;
		col.confSrcTabColIdx = -1;
		columns.add(col);
		return columns;
	}

	@SuppressWarnings("unused")
	private String getCurrentKeySpace(Connection conn)throws Exception {
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(CURRENT_KEYSPACE);
		rs.next();
		@SuppressWarnings("unchecked")
		String keySpace = (String)((Map<String,Object>)rs.getObject(1)).get(CURRENT_KEYSPACE);
		rs.close();
		st.close();
		return keySpace;
	}

}
