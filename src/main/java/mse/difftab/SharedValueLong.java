/**
 * 
 */
package mse.difftab;

/**
 * @author mse
 *
 */
class SharedValueLong {
	volatile long value;
	
	SharedValueLong(){
		this.value = 0;
	}
	
	SharedValueLong(long value){
		this.value = value;
	}	
}
