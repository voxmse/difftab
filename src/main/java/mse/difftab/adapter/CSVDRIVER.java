package mse.difftab.adapter;

import mse.difftab.ColInfo;
import mse.difftab.Adapter;
import mse.difftab.TabInfo;
import mse.difftab.ConfigValidationException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


public class CSVDRIVER implements Adapter {
	@Override
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception {
		nameFilter=(nameFilter==null)?"%":("%"+nameFilter.trim()+"%");

		List<TabInfo>tabs=new ArrayList<TabInfo>(); 
		TabInfo ti;

		ResultSet rs=null;
		
		try{
			rs=conn.getMetaData().getTables(null,null,nameFilter,null);

			while(rs.next()){
				ti=new TabInfo();
				ti.dbName=rs.getString("TABLE_NAME");
				ti.fullName=ti.dbName;
				ti.schema=null;
				ti.rows=-1;
				tabs.add(ti);
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

		nameFilter=nameFilter==null?"":nameFilter.trim();
		String query="SELECT * FROM "+table/*+" LIMIT 0"*/;

		try{
			rs=st.executeQuery(query);
			ResultSetMetaData md = rs.getMetaData();
			for(int i=1;i<=md.getColumnCount();i++){
				if(nameFilter.isEmpty()||nameFilter.equalsIgnoreCase(md.getColumnName(i))){
					ColInfo ci=new ColInfo();
					ci.dbName=rs.getMetaData().getColumnName(i);
					ci.fullName=ci.dbName;
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
}
