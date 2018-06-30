package mse.difftab.adapter;

import mse.difftab.ColInfo;
import mse.difftab.Adapter;
import mse.difftab.TabInfo;
import mse.difftab.ConfigValidationException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


public class CSVDRIVER implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		TabInfo ti;
		ResultSet rs=null;
		
		try{
			rs=conn.getMetaData().getTables(null,null,"%",null);
			while(rs.next()){
				if(nameFilter == null || rs.getString("TABLE_NAME").matches(nameFilter)) {
					ti=new TabInfo();
					ti.dbName=rs.getString("TABLE_NAME");
					ti.fullName=ti.dbName;
					ti.schema=null;
					ti.rows=-1;
					tabs.add(ti);
				}
			}
		}finally{
			try{rs.close();}catch(Exception e){}
		}
		return tabs;
	}

	@Override
	public List<ColInfo> getPK(Connection conn,String schema,String table,boolean isRowidPreffered)throws Exception{
		throw new ConfigValidationException("Can not determine key columns for the CSV table \""+table+"\".Please define them in the XML config file.");
	}

	@Override
	public List<ColInfo> getColumns(Connection conn,String schema,String table,String nameFilter)throws Exception {
		ArrayList<ColInfo>cols=new ArrayList<ColInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;

		String query="SELECT * FROM "+table/*+" LIMIT 0"*/;
		try{
			rs=st.executeQuery(query);
			ResultSetMetaData md = rs.getMetaData();
			int j = 0;
			for(int i=1;i<=md.getColumnCount();i++){
				if(nameFilter == null || md.getColumnName(i).matches(nameFilter)){
					ColInfo ci=new ColInfo();
					ci.colIdx = ++j;
					ci.dbName=md.getColumnName(i);
					ci.fullName=ci.dbName;
					ci.alias = ci.dbName.toUpperCase();
					ci.hashIdx = 1;
					ci.keyIdx = 0;
					ci.jdbcClassName = md.getColumnClassName(i);
					ci.confSrcTabColIdx = -1;
					cols.add(ci);
				}
			}
			rs.close();
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return cols;
	}

	@Override
	public boolean ColumnSetAndDataTypesAreFixed() {
		return true;
	}
	
	@Override
	public String getQuery(Connection conn, String schema, String table, List<ColInfo> columns){
		if(columns==null || columns.isEmpty()){
			return "SELECT * FROM "+table;
		}else{
			return "SELECT "+columns.stream().filter(ci -> ci.hashIdx>0 || ci.keyIdx>0).map(ci -> ci.fullName).collect(Collectors.joining(","))+" FROM "+table;
		}
	}
	
	@Override
	public List<ColInfo> getColumns(Connection conn,String query)throws Exception{
		Statement st = null;
		ResultSet rs = null;
		ResultSetMetaData md = null;
		List<ColInfo> columns = new ArrayList<ColInfo>();
		
		try {
			// get columns' metadata
			st=conn.createStatement();
			rs=st.executeQuery(query);
			md = rs.getMetaData();

			// get columns' data
			for (int i = 1; i <= md.getColumnCount(); i++) {
				ColInfo col = new ColInfo();
				col.colIdx = i;
				col.dbName = md.getColumnName(i);
				col.fullName = col.dbName;
				col.alias = col.dbName.toUpperCase();
				col.hashIdx = 1;
				col.keyIdx = 0;
				col.jdbcClassName = md.getColumnClassName(i);
				col.confSrcTabColIdx = -1;
				columns.add(col);
			}
			
			return columns;
		} finally {
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}		
	}
}
