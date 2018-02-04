package mse.difftab.adapter;

import mse.difftab.ColInfo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.sql.ResultSet;

import mse.difftab.Adapter;
import mse.difftab.TabInfo;

public class MYSQL implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		nameFilter=(nameFilter==null)?"":nameFilter.trim();
		if(!nameFilter.isEmpty()){
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(table_name='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","table_name")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		schemaFilter=(schemaFilter==null)?"?=database()":schemaFilter.trim();
		if(!schemaFilter.isEmpty()){
			schemaFilter=schemaFilter.replaceAll("(?<!\\\\)\\?","").equals(schemaFilter)?"(table_schema='"+schemaFilter+"')":"("+schemaFilter.replaceAll("(?<!\\\\)\\?","table_schema")+")";
			schemaFilter=schemaFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT table_name,table_name,table_schema,table_rows FROM information_schema.tables";
		if(!nameFilter.isEmpty()||!schemaFilter.isEmpty()) query+=" WHERE ";
		if(!nameFilter.isEmpty()) query+=nameFilter;
		if(!nameFilter.isEmpty()&&!schemaFilter.isEmpty()) query+=" AND ";
		if(!schemaFilter.isEmpty()) query+=schemaFilter;
		query+=" ORDER BY 1";
	
		try{
			rs=st.executeQuery(query);
			while(rs.next()){
				ti=new TabInfo();
				ti.dbName=rs.getString(1);
				ti.fullName=rs.getString(2);
				ti.schema=rs.getString(3);
				ti.rows=rs.getLong(4);
				if(rs.wasNull()) ti.rows=-1;
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
			rs=st.executeQuery("SELECT k.column_name FROM information_schema.table_constraints t LEFT JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema="+(schema==null?"database()":"'"+schema+"'")+" AND t.table_name='"+table+"' ORDER BY k.ordinal_position");
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
		return new ArrayList<ColInfo>();
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
			cols=getPK(conn,schema,table);
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
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(column_name='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","column_name")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT column_name FROM information_schema.columns WHERE table_name='"+table+"' AND table_schema='"+schema+"'";
		if(!nameFilter.isEmpty()) query+=" AND "+nameFilter;
		query+=" ORDER BY ordinal_position";
		
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
