/**
 * 
 */
package mse.difftab;

/**
 * @author m
 *
 */
@SuppressWarnings("serial")
public class RuntimeException extends Exception {
	public RuntimeException(String message,Throwable cause) {
		super("Runtime exception"+(((cause==null)||cause.getMessage()==null)?"":(":"+cause.getMessage())), cause);
	}
	public RuntimeException(String message) {
		super("Runtime exception"+(message==null?"":(":"+message)));
	}
	
	
}
