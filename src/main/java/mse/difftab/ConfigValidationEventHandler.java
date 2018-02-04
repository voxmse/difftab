package mse.difftab;

import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;

public class ConfigValidationEventHandler implements ValidationEventHandler {
	StringBuffer errMsg;
	
	public ConfigValidationEventHandler(StringBuffer errMsg){
		this.errMsg=errMsg;
	}
	
	@Override
	public boolean handleEvent(ValidationEvent event) {
		errMsg.append(event.toString());
		return false;
	}
}
