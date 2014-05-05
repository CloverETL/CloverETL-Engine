/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.component.fileoperation;


public class PooledFTPOperationHandlerTest extends FTPOperationHandlerTest {
	
	protected PooledFTPOperationHandler handler = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new PooledFTPOperationHandler();
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		handler = null;
	}

	@Override
	public void testGetPriority() {
//		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.copy(PooledFTPOperationHandler.FTP_SCHEME, PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.move(PooledFTPOperationHandler.FTP_SCHEME, PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(PooledFTPOperationHandler.FTP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(PooledFTPOperationHandler.FTP_SCHEME)));
	}

	@Override
	public void testCanPerform() {
//		assertTrue(handler.canPerform(Operation.copy(PooledFTPOperationHandler.FTP_SCHEME, PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(PooledFTPOperationHandler.FTP_SCHEME, PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(PooledFTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(PooledFTPOperationHandler.FTP_SCHEME)));
	}

}
