package org.jetel.util.protocols.smb2;

import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.SMB2OperationHandler;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.SMB2ConnectionFactory;
import org.jetel.plugin.PluginActivator;

public class Activator extends PluginActivator {

	@Override
	public void activate() {
		ConnectionPool.getConnectionFactory().addFactory(new SMB2ConnectionFactory());
		FileManager.getInstance().registerHandler(new SMB2OperationHandler());
	}

	@Override
	public void deactivate() {
	}

}
