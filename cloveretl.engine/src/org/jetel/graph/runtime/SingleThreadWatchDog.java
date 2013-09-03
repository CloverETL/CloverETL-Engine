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
package org.jetel.graph.runtime;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphAnalyzer;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MiscUtils;


/**
 * Alternative implementation of {@link WatchDog}. This kind of WatchDog does not run
 * each component in separate thread, but ensures, that the complete transformation is
 * performed in a single thread.
 * Slightly different progress logging is provided by this watchdog. Operation {@link #abort()}
 * is not supported. Instead, just call interrupt on the single thread.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.6.2013
 */
public class SingleThreadWatchDog extends WatchDog {

    private static Logger logger = Logger.getLogger(SingleThreadWatchDog.class);

	public SingleThreadWatchDog(TransformationGraph graph, GraphRuntimeContext runtimeContext) {
		super(graph, runtimeContext);
	}

	@Override
	protected Result executePhase(Phase phase) {
		//preExecute the given phase
		try {
			phase.preExecute();
		} catch (ComponentNotReadyException e) {
			ExceptionUtils.logException(logger, "Phase pre-execute initialization failed", e);
			setCauseException(e);
			setCauseGraphElement(e.getGraphElement());
			return Result.ERROR;
		}
		Result phaseStatus = Result.N_A;
		try {
			//find a correct component order in which the transformation will be performed - component by component
			List<Node> components = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(phase.getNodes().values()));
			//preExecute all components in the given phase
			for (Node component : components) {
				try {
					logger.info("Component " + component + " pre-executed.");
					component.setNodeThread(Thread.currentThread()); // correct thread should be preset for preExecution - this is necessary to be compatible with multi-thread execution 
					component.preExecute();
					component.setNodeThread(null);
				} catch (Exception e) {
					component.setResultCode(Result.ERROR);
					ExceptionUtils.logException(logger, "Component " + component + " pre-execute initialization failed", e);
					setCauseException(e);
					setCauseGraphElement(component);
					return Result.ERROR;
				}
			}
			phaseStatus = Result.READY;
			//execute each component by component in topological order
			for (Node component : components) {
				logger.info("Component " + component + " executed.");
				component.run();
				if (component.getResultCode() == Result.FINISHED_OK) {
					//do nothing just run next component
					logger.info("Component " + component + " finished successfully (" + MiscUtils.getInOutRecordsMessage(component) + ").");
				} else if (component.getResultCode() == Result.ABORTED) {
					return Result.ABORTED;
				} else if (component.getResultCode() == Result.ERROR) {
					ExceptionUtils.logException(logger, null, component.getResultException());
					setCauseException(component.getResultException());
					setCauseGraphElement(component);
					return Result.ERROR;
				}
			}
			phaseStatus = Result.FINISHED_OK;
		} finally {
        	//postExecute() invocation
        	try {
        		phase.postExecute();
        	} catch (ComponentNotReadyException e) {
    			ExceptionUtils.logException(logger, "Phase post-execute finalization failed", e);
    			setCauseException(e);
    			setCauseGraphElement(e.getGraphElement());
    			return Result.ERROR;
        	}
		}
		return phaseStatus;
	}
	
	@Override
	public void abort() {
		throw new UnsupportedOperationException("single thread graph execution does not support abortation");
	}
	
}

