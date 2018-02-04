package mse.difftab.adapter;

import mse.difftab.ColInfo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.sql.ResultSet;

import mse.difftab.Adapter;
import mse.difftab.TabInfo;

public class POSTGRESQL implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		nameFilter=(nameFilter==null)?"":nameFilter.trim();
		if(!nameFilter.isEmpty()){
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(tablename='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","tablename")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		schemaFilter=(schemaFilter==null)?"?=current_schema":schemaFilter.trim();
		if(!schemaFilter.isEmpty()){
			schemaFilter=schemaFilter.replaceAll("(?<!\\\\)\\?","").equals(schemaFilter)?"(schemaname='"+schemaFilter+"')":"("+schemaFilter.replaceAll("(?<!\\\\)\\?","schemaname")+")";
			schemaFilter=schemaFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT * FROM("+
			"SELECT t.tablename,'\"'||t.schemaname||'\".\"'||t.tablename||'\"',t.schemaname,coalesce(st.n_live_tup,-1) FROM pg_tables t LEFT JOIN pg_stat_all_tables st ON t.schemaname=st.schemaname AND t.tablename=st.relname"+
//			" UNION ALL "+
//			"SELECT v.viewname,'\"'||v.schemaname||'\".\"'||v.viewname||'\"',v.schemaname,-1 FROM pg_views v"+
			")t";
		if(!nameFilter.isEmpty()||!schemaFilter.isEmpty()) query+=" WHERE ";
		if(!nameFilter.isEmpty()) query+=nameFilter;
		if(!nameFilter.isEmpty()&&!schemaFilter.isEmpty()) query+=" AND ";
		if(!schemaFilter.isEmpty()) query+=schemaFilter;
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
			rs=st.executeQuery("SELECT a.attname FROM pg_class c JOIN pg_tables t ON c.relname=t.tablename JOIN pg_roles r ON c.relowner=r.oid AND r.rolname=t.tableowner JOIN pg_index i ON i.indrelid=c.oid JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) WHERE i.indrelid='\""+table+"\"'::regclass AND i.indisprimary AND t.schemaname="+(schema==null?"current_schema":"'"+schema+"'")+" AND NOT a.attisdropped ORDER BY a.attnum");
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
		List<ColInfo>cols=new ArrayList<ColInfo>();
		Statement st=conn.createStatement();
		ResultSet rs=null;
		try{
			rs=st.executeQuery("SELECT COUNT(*) FROM pg_tables t WHERE t.tablename='"+table+"' AND t.schemaname="+(schema==null?"current_schema":"'"+schema+"'"));
			while(rs.next()){
				if(rs.getInt(1)>0){
					ColInfo ci=new ColInfo();
					ci.dbName="ctid";
					ci.fullName=ci.dbName;
					ci.hashIdx=0;
					cols.add(ci);
				}
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
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(a.attname='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","a.attname")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT a.attname FROM pg_attribute a JOIN pg_class c ON a.attnum>0 AND c.oid=a.attrelid JOIN pg_tables t ON c.relname=t.tablename JOIN pg_roles r ON c.relowner=r.oid AND r.rolname=t.tableowner AND t.tablename='"+table+"' AND schemaname='"+schema+"'";
		if(!nameFilter.isEmpty()) query+=" AND "+nameFilter;
		query+=" WHERE NOT a.attisdropped ORDER BY a.attnum";
		
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
