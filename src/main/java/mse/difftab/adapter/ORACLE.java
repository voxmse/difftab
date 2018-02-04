package mse.difftab.adapter;

import mse.difftab.ColInfo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.sql.ResultSet;

import mse.difftab.Adapter;
import mse.difftab.TabInfo;

public class ORACLE implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		nameFilter=(nameFilter==null)?"":nameFilter.trim();
		if(!nameFilter.isEmpty()){
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(TABLE_NAME='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","TABLE_NAME")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		schemaFilter=(schemaFilter==null)?"?=USER":schemaFilter.trim();
		if(!schemaFilter.isEmpty()){
			schemaFilter=schemaFilter.replaceAll("(?<!\\\\)\\?","").equals(schemaFilter)?"(OWNER='"+schemaFilter+"')":"("+schemaFilter.replaceAll("(?<!\\\\)\\?","OWNER")+")";
			schemaFilter=schemaFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT table_name,'\"'||owner||'\".\"'||table_name||'\"',owner,NVL(num_rows,-1) FROM all_tables WHERE temporary='N' AND secondary='N' AND nested='NO'";
		if(!nameFilter.isEmpty()) query+=" AND "+nameFilter;
		if(!schemaFilter.isEmpty()) query+=" AND "+schemaFilter;
		try{
			rs=st.executeQuery(query);
			while(rs.next()){
				ti=new TabInfo();
				ti.dbName=rs.getString(1);
				ti.fullName=rs.getString(2);
				ti.schema=rs.getString(3);
				ti.rows=rs.getLong(4);
				tabs.add(ti);
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
			if(schema==null||schema.isEmpty()){
				rs=st.executeQuery("SELECT column_name FROM user_cons_columns cc JOIN user_constraints c ON cc.owner=c.owner AND cc.table_name=c.table_name AND cc.constraint_name=c.constraint_name AND c.constraint_type='P' AND c.table_name='"+table+"'");
			}else{
				rs=st.executeQuery("SELECT column_name FROM all_cons_columns cc JOIN all_constraints c ON cc.owner=c.owner AND cc.table_name=c.table_name AND cc.constraint_name=c.constraint_name AND c.constraint_type='P' AND c.owner='"+schema+"' AND c.table_name='"+table+"'");
			}
			while(rs.next()){
				ColInfo ci;
				ci=new ColInfo();
				ci.dbName=rs.getString(1);
				ci.fullName="\""+ci.dbName+"\"";
				ci.hashIdx=1;
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
			if(schema==null||schema.isEmpty()){
				rs=st.executeQuery("SELECT iot_type FROM user_tables WHERE table_name='"+table+"'");
			}else{
				rs=st.executeQuery("SELECT iot_type FROM all_tables WHERE owner='"+schema+"' AND table_name='"+table+"'");
			}
			rs.next();
			rs.getString(1);
			if(rs.wasNull()){
				cols=new ArrayList<ColInfo>();
				ColInfo ci=new ColInfo();
				ci.dbName="ROWID";
				ci.fullName=ci.dbName;
				ci.hashIdx=0;
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
				return getPK(conn,schema,table);
			}else{
				return cols;
			}
		}else{
			cols= getPK(conn,schema,table);
			if(cols.size()==0){
				return getROWID(conn,schema,table);
			}else{
				return cols;
			}
		}

	}

	@Override
	public List<ColInfo> getColumns(Connection conn,String schema,String table,String nameFilter)throws Exception {
		ArrayList<ColInfo>cols=new ArrayList<ColInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;

		nameFilter=nameFilter==null?"":nameFilter.trim();
		if(!nameFilter.isEmpty()){
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(COLUMN_NAME='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","COLUMN_NAME")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT column_name FROM all_tab_cols WHERE hidden_column='NO' AND virtual_column='NO' AND table_name='"+table+"' AND owner='"+schema+"'";
		if(!nameFilter.isEmpty()) query+=" AND "+nameFilter;
		query+=" ORDER BY column_id";
		
		try{
			rs=st.executeQuery(query);
			while(rs.next()){
				ColInfo ci=new ColInfo();
				ci.dbName=rs.getString(1);
				ci.fullName="\""+ci.dbName+"\"";
				ci.hashIdx=1;
				cols.add(ci);
			}
			rs.close();
		}finally{
			try{rs.close();}catch(Exception e){}
			try{st.close();}catch(Exception e){}
		}
		return cols;
	}

}
