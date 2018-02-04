package mse.difftab;

class SqlQueryExcutor extends Thread {
	DiffTab app;
	String srcName;
	java.sql.PreparedStatement pst;
	
	SqlQueryExcutor(String srcName, DiffTab app, java.sql.PreparedStatement pst){
		this.app=app;
		this.srcName=srcName;
		this.pst=pst;
	}
	
	public void run(){
		try{
			app.writeLog("exec:"+srcName+":SQL execution is started");
			pst.execute();
			app.writeLog("exec:"+srcName+":SQL execution is finished");
		}catch(Exception e){
			app.registerFailure(new RuntimeException("exec:"+srcName+":SQL execution failed:"+e.getMessage(),e));
		}
	}
}
