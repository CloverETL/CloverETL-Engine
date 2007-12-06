package org.jetel.connection;

public enum MultiAutoKeySupported{
	oracle,
	db2;
//	mssql TODO
//	postgre - driver does not support auto generated keys

	public static boolean isSupported(String driverName) {
		String driver = driverName.trim().toLowerCase();
		for (MultiAutoKeySupported supportedDatabase : MultiAutoKeySupported.values()) {
			if (driver.contains(supportedDatabase.name())) return true;
		}
		return false;
	}
}
