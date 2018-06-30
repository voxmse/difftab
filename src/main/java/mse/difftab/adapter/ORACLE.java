package mse.difftab.adapter;

import mse.difftab.ColInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import mse.difftab.Adapter;
import mse.difftab.TabInfo;

public class ORACLE implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		String query;
		if(schemaFilter == null) {
			query = "SELECT table_name,'\"'||USER||'\".\"'||table_name||'\"',USER,NVL(num_rows,-1) FROM user_tables WHERE temporary='N' AND secondary='N' AND nested='NO'";
		}else{
			query = "SELECT table_name,'\"'||owner||'\".\"'||table_name||'\"',owner,NVL(num_rows,-1) FROM all_tables WHERE temporary='N' AND secondary='N' AND nested='NO'";
		}
		try{
			rs=st.executeQuery(query);
			while(rs.next()){
				if((schemaFilter == null || rs.getString(3).matches(schemaFilter)) && (nameFilter == null || rs.getString(1).matches(nameFilter))){
					ti=new TabInfo();
					ti.dbName=rs.getString(1);
					ti.fullName=rs.getString(2);
					ti.schema=rs.getString(3);
					ti.rows=rs.getLong(4);
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

	private List<ColInfo> getPK(Connection conn,String schema,String table)throws Exception {
		List<ColInfo>cols=new ArrayList<ColInfo>();
		Statement st=conn.createStatement();
		ResultSet rs=null;
		try{
			if(schema == null){
				rs = st.executeQuery("SELECT column_name FROM user_cons_columns cc JOIN user_constraints c ON cc.owner=c.owner AND cc.table_name=c.table_name AND cc.constraint_name=c.constraint_name AND c.constraint_type='P' AND c.table_name='"+table+"'");
			}else{
				rs = st.executeQuery("SELECT column_name FROM all_cons_columns cc JOIN all_constraints c ON cc.owner=c.owner AND cc.table_name=c.table_name AND cc.constraint_name=c.constraint_name AND c.constraint_type='P' AND c.owner='"+schema+"' AND c.table_name='"+table+"'");
			}
			int i = 0;
			while(rs.next()){
				ColInfo ci;
				ci=new ColInfo();
				ci.colIdx = ++i;
				ci.dbName = rs.getString(1);
				ci.fullName="\""+ci.dbName+"\"";
				ci.alias = ci.dbName.toUpperCase();
				ci.hashIdx = 1;
				ci.keyIdx = 1;
				ci.confSrcTabColIdx = -1;
				cols.add(ci);
			}
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return cols;
	}

	private List<ColInfo> getROWID(Connection conn,String schema,String table)throws Exception {
		List<ColInfo>cols;
		Statement st=conn.createStatement();
		ResultSet rs=null;
		try{
			if(schema == null){
				rs=st.executeQuery("SELECT iot_type FROM user_tables WHERE table_name='"+table+"'");
			}else{
				rs=st.executeQuery("SELECT iot_type FROM all_tables WHERE owner='"+schema+"' AND table_name='"+table+"'");
			}
			rs.next();
			rs.getString(1);
			if(rs.wasNull()){
				cols=new ArrayList<ColInfo>();
				ColInfo ci=new ColInfo();
				ci.colIdx = 1;
				ci.dbName="ROWID";
				ci.fullName=ci.dbName;
				ci.alias = ci.dbName.toUpperCase();
				ci.hashIdx = 0;
				ci.keyIdx = 1;
				ci.confSrcTabColIdx = -1;	
				cols.add(ci);
			}else{
				cols=getPK(conn,schema,table);
			}
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return cols;		
	}

	@Override
	public List<ColInfo> getPK(Connection conn,String schema,String table,boolean isRowidPreffered)throws Exception{
		List<ColInfo> cols;
		if(isRowidPreffered){
			cols=getROWID(conn,schema,table);
			if(cols.size()==0){
				cols= getPK(conn,schema,table);
			}
		}else{
			cols= getPK(conn,schema,table);
			if(cols.size()==0){
				cols=getROWID(conn,schema,table);
			}
		}
		fillJavaTypeForColumns(conn, getQuery(conn, schema, table, cols), cols);
		return cols;

	}

	@Override
	public List<ColInfo> getColumns(Connection conn,String schema,String table,String nameFilter)throws Exception {
		ArrayList<ColInfo>cols=new ArrayList<ColInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;

		String query = "SELECT column_name FROM all_tab_cols WHERE hidden_column='NO' AND virtual_column='NO' AND table_name='"+table+"' AND owner='"+schema+"' ORDER BY column_id";
		try{
			rs=st.executeQuery(query);
			int i = 0;
			while(rs.next()){
				if(nameFilter == null || rs.getString(1).matches(nameFilter)) {
					ColInfo ci=new ColInfo();
					ci.colIdx = ++i;
					ci.dbName=rs.getString(1);
					ci.fullName="\""+ci.dbName+"\"";
					ci.alias = ci.dbName.toUpperCase();
					ci.hashIdx = 1;
					ci.keyIdx = 0;
					ci.confSrcTabColIdx = -1;
				}
			}
			rs.close();
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		
		fillJavaTypeForColumns(conn, getQuery(conn, schema, table, cols), cols);
		return cols;
	}

	private void fillJavaTypeForColumns(Connection conn, String query, List<ColInfo> columns)throws Exception {
		PreparedStatement ps = null;
		ResultSetMetaData md = null;

		try {
			if(columns.size()>0) {
				// get columns' metadata
				ps = conn.prepareStatement(query);
				md = ps.getMetaData();

				// get columns' data
				for (int i = 1; i <= columns.size(); i++)
					columns.get(i - 1).jdbcClassName = md.getColumnClassName(i);
			}
		} finally {
			try{ps.close();}catch(Exception e){}
		}
	}
	
	@Override
	public boolean ColumnSetAndDataTypesAreFixed() {
		return true;
	}

	@Override
	public String getQuery(Connection conn, String schema, String table, List<ColInfo> columns){
		if(columns==null || columns.isEmpty()){
			return "SELECT * FROM "+(schema==null?"":("\""+schema+"\"."))+"\""+table+"\"";
		}else{
			return "SELECT "+columns.stream().map(ci -> ci.fullName).collect(Collectors.joining(","))+" FROM "+(schema==null?"":("\""+schema+"\"."))+"\""+table+"\"";
		}
	}

	@Override
	public List<ColInfo> getColumns(Connection conn,String query)throws Exception{
		PreparedStatement ps = null;
		ResultSetMetaData md = null;
		List<ColInfo> columns = new ArrayList<ColInfo>();
		
		try {
			// get columns' metadata
			ps = conn.prepareStatement(query);
			md = ps.getMetaData();

			// get columns' data
			for (int i = 1; i <= md.getColumnCount(); i++) {
				ColInfo col = new ColInfo();
				col.colIdx = i;
				col.dbName = md.getColumnName(i);
				col.fullName = "\""+col.dbName+"\"";
				col.alias = col.dbName.toUpperCase();
				col.jdbcClassName = md.getColumnClassName(i);
				col.keyIdx = 0;
				col.hashIdx = 1;
				col.confSrcTabColIdx = -1;
				columns.add(col);
			}
			
			return columns;
		} finally {
			try{ps.close();}catch(Exception e){}
		}		
	}

}
