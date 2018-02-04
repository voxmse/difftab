package mse.difftab;

import java.sql.Connection;
import java.util.List;


public interface Adapter {
	public List<TabInfo> getTables(Connection conn,String schemaFilter,String nameFilter)throws Exception;
	public List<ColInfo> getColumns(Connection conn,String schema,String table,String nameFilter)throws Exception;
	public List<ColInfo> getPK(Connection conn,String schema,String table,boolean isRowidPreffered)throws Exception;
}
