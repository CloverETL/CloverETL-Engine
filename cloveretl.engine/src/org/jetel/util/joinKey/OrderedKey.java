package org.jetel.util.joinKey;

import org.jetel.enums.OrderEnum;

/**
 * Data holder class for ordered key
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 * (c) OpenSys (www.opensys.eu)
 */
public class OrderedKey {

	private String keyName;
	private OrderEnum ordering;
	
	public OrderedKey(String keyName, OrderEnum ordering) {
		super();
		this.keyName = keyName;
		this.ordering = ordering;
	}
	public String getKeyName() {
		return keyName;
	}
	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}
	public OrderEnum getOrdering() {
		return ordering;
	}
	public void setOrdering(OrderEnum ordering) {
		this.ordering = ordering;
	}

	@Override
	public String toString() {
		return keyName + ":" + ordering.toString();
	}
	
}
