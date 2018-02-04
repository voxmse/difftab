package mse.difftab.adapter;

import mse.difftab.ColInfo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.sql.ResultSet;

import mse.difftab.Adapter;
import mse.difftab.TabInfo;

public class MSSQL implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		Statement st=conn.createStatement();
		ResultSet rs=null;
		TabInfo ti;

		nameFilter=(nameFilter==null)?"":nameFilter.trim();
		if(!nameFilter.isEmpty()){
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(ta.name='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","ta.name")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		schemaFilter=(schemaFilter==null)?"?=current_schema":schemaFilter.trim();
		if(!schemaFilter.isEmpty()){
			schemaFilter=schemaFilter.replaceAll("(?<!\\\\)\\?","").equals(schemaFilter)?"(sc.name='"+schemaFilter+"')":"("+schemaFilter.replaceAll("(?<!\\\\)\\?","sc.name")+")";
			schemaFilter=schemaFilter.replaceAll("\\\\\\?","\\?");
		}
		
		String query="SELECT ta.name,'['+sc.name+'].['+ta.name+']',sc.name schemaname,SUM(pa.rows)"+
		" FROM sys.tables ta JOIN sys.partitions pa ON pa.OBJECT_ID=ta.OBJECT_ID JOIN sys.schemas sc ON ta.schema_id=sc.schema_id"+
		" WHERE ta.is_ms_shipped=0 AND pa.index_id IN(1,0)";
		if(!nameFilter.isEmpty()) query += " AND " + nameFilter;
		if(!schemaFilter.isEmpty()) query += " AND " + schemaFilter;
		query += " GROUP BY ta.name,'['+sc.name+'].['+ta.name+']',sc.name";

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
			rs=st.executeQuery("SELECT tc.name FROM sys.schemas s JOIN sys.tables t ON s.schema_id=t.schema_id JOIN sys.indexes i ON t.object_id=i.object_id JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id JOIN sys.columns tc ON ic.object_id=tc.object_id AND ic.column_id=tc.column_id WHERE t.name='"+table+"' AND s.name="+(schema==null?"SCHEMA_NAME()":"'"+schema+"'")+" AND i.is_primary_key=1 ORDER BY ic.key_ordinal");
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
			rs=st.executeQuery("SELECT COUNT(*) FROM sys.tables ta JOIN sys.partitions pa ON pa.OBJECT_ID=ta.OBJECT_ID JOIN sys.schemas sc ON ta.schema_id=sc.schema_id WHERE ta.is_ms_shipped=0 AND pa.index_id IN(1,0) AND ta.name='"+table+"' AND sc.name="+(schema==null?"SCHEMA_NAME()":"'"+schema+"'"));

			while(rs.next()){
				if(rs.getInt(1)>0){
					ColInfo ci=new ColInfo();
					ci.dbName="sys.fn_PhysLocFormatter(%%physloc%%)";
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
			nameFilter=nameFilter.replaceAll("(?<!\\\\)\\?","").equals(nameFilter)?"(c.name='"+nameFilter+"')":"("+nameFilter.replaceAll("(?<!\\\\)\\?","c.name")+")";
			nameFilter=nameFilter.replaceAll("\\\\\\?","\\?");
		}
		String query="SELECT c.name FROM sys.columns c JOIN sys.objects o ON o.object_id=c.object_id WHERE o.type IN('U','V') AND o.name='"+table+"' AND OBJECT_SCHEMA_NAME(c.object_id)='"+schema+"'";

		if(!nameFilter.isEmpty()) query+=" AND "+nameFilter;
		query+=" ORDER BY c.column_id";
		
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
