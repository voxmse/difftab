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

public class MONGODB implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		try{
			rs=st.executeQuery("{listCollections:1"+(nameFilter==null?"":(",filter:"+nameFilter))+"}");
			while(rs.next()){
				ti=new TabInfo();
				@SuppressWarnings("unchecked")
				Map<String,Object> collectionInfo = (Map<String,Object>)rs.getObject(1);
				ti.dbName=(String)collectionInfo.get("name");
				ti.fullName=ti.dbName;
				ti.schema="";
				tabs.add(ti);
			}
			rs.close();
			
			for(TabInfo ti2 : tabs) {
				rs=st.executeQuery("{collStats :'"+ti2.dbName+"'}");
				if(rs.next()) {
					@SuppressWarnings("unchecked")
					Map<String,Object> collectionStats = (Map<String,Object>)rs.getObject(1);
					ti2.rows=(Integer)collectionStats.get("count");
				}
			}
			rs.close();
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return tabs;
	}

	private List<ColInfo> getPK(Connection conn,String schema,String table)throws Exception {
		List<ColInfo>cols=new ArrayList<ColInfo>();
		Statement st=conn.createStatement();
		ResultSet rs=null;
		try{
			rs=st.executeQuery("{listIndexes :'"+table+"'}");
			while(rs.next()){
				@SuppressWarnings("unchecked")
				Map<String,Object> indexInfo = (Map<String,Object>)rs.getObject(1);
				if(indexInfo.containsKey("unique") && (Boolean)indexInfo.get("unique") && !(indexInfo.containsKey("sparse") && (Boolean)indexInfo.get("sparse"))) {
					@SuppressWarnings("unchecked")
					Map<String,Object> keys = (Map<String,Object>)indexInfo.get("key");
					if(keys.size()>1 || !keys.containsKey("_id")) {
						ColInfo ci;
						int i = 0;
						for(String key : keys.keySet()) {
							ci=new ColInfo();
							ci.colIdx = ++i;
							ci.dbName = key;
							ci.fullName = key;
							ci.alias = ci.dbName.toUpperCase();
							ci.hashIdx = 1;
							ci.keyIdx = 1;
							ci.jdbcClassName = "MAP_AS_COLUMNSET";
							ci.confSrcTabColIdx = -1;
							cols.add(ci);
						}
						break;
					}
				}
			}
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return cols;
	}

	private List<ColInfo> getROWID(Connection conn,String schema,String table)throws Exception {
		List<ColInfo>cols=new ArrayList<ColInfo>();
		ColInfo ci=new ColInfo();
		ci.colIdx = 1;
		ci.dbName = "_id";
		ci.fullName = "_id";
		ci.alias = ci.dbName.toUpperCase();
		ci.hashIdx = 0;
		ci.keyIdx = 1;
		ci.jdbcClassName = MAP_AS_COLUMNSET;
		ci.confSrcTabColIdx = -1;
		cols.add(ci);
		return cols;		
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
		boolean selectedColumnsOnly = columns.stream().filter(ci -> ci.dbName.equals(ColInfo.OVERALL_COLUMN_NAME) && ci.hashIdx<=0 && ci.keyIdx<=0).count()>0;
		long notIdExcludedColumns = columns.stream().filter(ci -> !ci.dbName.equals(ColInfo.OVERALL_COLUMN_NAME) && !ci.dbName.equals("_id") && ci.hashIdx<=0 && ci.keyIdx<=0).count();
		long includedColumns = columns.stream().filter(ci -> !ci.dbName.equals(ColInfo.OVERALL_COLUMN_NAME) && (ci.hashIdx>0 || ci.keyIdx>0)).count();

		if(selectedColumnsOnly && ((notIdExcludedColumns==0 && includedColumns>0)||(notIdExcludedColumns>0 && includedColumns==0))){
			return "{find:\""+table+"\",projection:{"+
					columns.stream().filter(ci -> !ci.dbName.equals(ColInfo.OVERALL_COLUMN_NAME)).map(ci -> ci.fullName+":"+((ci.keyIdx>0||ci.hashIdx>0)?"1":"0")).collect(Collectors.joining(","))+
					"}}";
		}else{
			return "{find:\""+table+"\"}";
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


}
