package org.jetel.ctl.data;

public class UnknownTypeException extends RuntimeException {

	private String type;


	public UnknownTypeException(String type) {
		super();
		this.type = type;
	}
	
	public String getType() {
		return type;
	}
	
	
}
