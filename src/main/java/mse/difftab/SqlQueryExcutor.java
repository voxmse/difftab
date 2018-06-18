package mse.difftab;

class SqlQueryExcutor extends Thread {
	DiffTab app;
	String srcName;
	java.sql.Statement st;
	String query;
	
	SqlQueryExcutor(String srcName, DiffTab app, java.sql.Statement st, String query){
		this.app=app;
		this.srcName=srcName;
		this.st=st;
		this.query = query;
	}
	
	public void run(){
		try{
			app.writeLog("exec:"+srcName+":SQL execution is started");
			st.executeQuery(query);
			app.writeLog("exec:"+srcName+":SQL execution is finished");
		}catch(Exception e){
e.printStackTrace();			
			app.registerFailure(new RuntimeException("exec:"+srcName+":SQL execution failed:"+e.getMessage(),e));
		}
	}
}
