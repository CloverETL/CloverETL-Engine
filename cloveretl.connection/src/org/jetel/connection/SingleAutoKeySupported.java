package org.jetel.connection;

public enum SingleAutoKeySupported{
	mysql,
	informix;
//	mssql TODO
//	postgre - driver does not support auto generated keys
	
	public static boolean isSupported(String driverName) {
		String driver = driverName.trim().toLowerCase();
		for (SingleAutoKeySupported supportedDatabase : SingleAutoKeySupported.values()) {
			if (driver.contains(supportedDatabase.name())) return true;
		}
		return false;
	}
}
