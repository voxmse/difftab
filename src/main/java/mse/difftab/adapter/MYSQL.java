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

public class MYSQL implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		String query="SELECT table_name,CONCAT('`',table_name,'`'),table_schema,table_rows FROM information_schema.tables";
		if(schemaFilter == null) {
			query += " WHERE table_schema=ifnull(database(),'information_schema')";
		}
		query += " ORDER BY 1";
		try{
			rs=st.executeQuery(query);
			while(rs.next()){
				if((schemaFilter == null || rs.getString(3).matches(schemaFilter)) && (nameFilter == null || rs.getString(1).matches(nameFilter))){
					ti=new TabInfo();
					ti.dbName=rs.getString(1);
					ti.fullName=rs.getString(2);
					ti.schema=rs.getString(3);
					ti.rows=rs.getLong(4);
					if(rs.wasNull()) ti.rows=-1;
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
			rs=st.executeQuery("SELECT k.column_name FROM information_schema.table_constraints t LEFT JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema="+(schema==null?"database()":"'"+schema+"'")+" AND t.table_name='"+table+"' ORDER BY k.ordinal_position");
			int i =0;
			while(rs.next()){
				ColInfo ci;
				ci=new ColInfo();
				ci.colIdx = ++i;
				ci.dbName = rs.getString(1);
				ci.fullName="`"+ci.dbName+"`";
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
			cols=getPK(conn,schema,table);
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

		String query="SELECT column_name FROM information_schema.columns WHERE table_name='"+table+"' AND table_schema='"+schema+"' ORDER BY ordinal_position";
		try{
			rs=st.executeQuery(query);
			int i = 0;
			while(rs.next()){
				if(nameFilter == null || rs.getString(1).matches(nameFilter)) {
					ColInfo ci=new ColInfo();
					ci.colIdx = ++i;
					ci.dbName=rs.getString(1);
					ci.fullName="`"+ci.dbName+"`";
					ci.alias = ci.dbName.toUpperCase();
					ci.hashIdx = 1;
					ci.keyIdx = 0;
					ci.confSrcTabColIdx = -1;				
					cols.add(ci);
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

	@Override
	public boolean ColumnSetAndDataTypesAreFixed() {
		return true;
	}

	@Override
	public String getQuery(Connection conn, String schema, String table, List<ColInfo> columns){
		if(columns==null || columns.isEmpty()){
			return "SELECT * FROM "+(schema==null?"":("`"+schema+"`."))+"`"+table+"`";
		}else{
			return "SELECT "+columns.stream().map(ci -> ci.fullName).collect(Collectors.joining(","))+" FROM "+(schema==null?"":("`"+schema+"`."))+"`"+table+"`";
		}
	}
	
	@Override
	public List<ColInfo> getColumns(Connection conn,String query)throws Exception{
		PreparedStatement ps = null;
		ResultSetMetaData md = null;
		List<ColInfo> columns = new ArrayList<ColInfo>();
		
		try {
			if(columns.size()>0){
				// get columns' metadata
				ps = conn.prepareStatement(query);
				md = ps.getMetaData();

				// get columns' data
				for (int i = 1; i <= md.getColumnCount(); i++) {
					ColInfo col = new ColInfo();
					col.colIdx = i;
					col.dbName = md.getColumnName(i);
					col.fullName = "`"+col.dbName+"`";
					col.alias = col.dbName.toUpperCase();
					col.jdbcClassName = md.getColumnClassName(i);
					col.keyIdx = 0;
					col.hashIdx = 1;
					col.confSrcTabColIdx = -1;
					columns.add(col);
				}
			}
			
			return columns;
		} finally {
			try{ps.close();}catch(Exception e){}
		}		
	}
	
	private void fillJavaTypeForColumns(Connection conn, String query, List<ColInfo> columns)throws Exception {
		PreparedStatement ps = null;
		ResultSetMetaData md = null;

		try {
			// get columns' metadata
			ps = conn.prepareStatement(query);
			md = ps.getMetaData();

			// get columns' data
			for (int i = 1; i <= columns.size(); i++)
				columns.get(i - 1).jdbcClassName = md.getColumnClassName(i);
		} finally {
			try{ps.close();}catch(Exception e){}
		}
		
	}
}
