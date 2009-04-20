/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.graph.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.main.runGraph;
import org.jetel.util.file.FileUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 11, 2008
 */
public class PrimitiveAuthorityProxy implements IAuthorityProxy {

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#getSharedSequence(org.jetel.data.sequence.Sequence)
	 */
	public Sequence getSharedSequence(Sequence sequence) {
		return sequence;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.runtime.IAuthorityProxy#freeSharedSequence(org.jetel.data.sequence.Sequence)
	 */
	public void freeSharedSequence(Sequence sequence) {
		sequence.free();
	}

	/**
	 * Implementation taken from original RunGraph component created by Juraj Vicenik.
	 * 
	 * @see org.jetel.graph.runtime.IAuthorityProxy#executeGraph(long, java.lang.String)
	 */
	public RunResult executeGraph(long runId, String graphFileName) {
        RunResult rr = new RunResult();

		InputStream in = null;		

		try {
            in = Channels.newInputStream(FileUtils.getReadableChannel(null, graphFileName));
        } catch (IOException e) {
        	rr.description = "Error - graph definition file can't be read: " + e.getMessage();
        	rr.result = Result.ERROR;
        	return rr;
        }
        
        GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
        Future<Result> futureResult = null;                

        // TODO - hotfix - clover can't run two graphs simultaneously with enable edge debugging
		// after resolve issue 1748 (http://home.javlinconsulting.cz/view.php?id=1748) next line should be removed
        runtimeContext.setDebugMode(false);
        
        TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext.getAdditionalProperties());
        } catch (XMLConfigurationException e) {
        	rr.description = "Error in reading graph from XML !" + e.getMessage();
        	rr.result = Result.ERROR;
        	return rr;
        } catch (GraphConfigurationException e) {
        	rr.description = "Error - graph's configuration invalid !" + e.getMessage();
        	rr.result = Result.ERROR;
        	return rr;
		} 

        long startTime = System.currentTimeMillis();
        Result result = Result.N_A;
        try {
    		try {
    			EngineInitializer.initGraph(graph, runtimeContext);
    			futureResult = runGraph.executeGraph(graph, runtimeContext);

    		} catch (ComponentNotReadyException e) {
    			rr.description = "Error during graph initialization: " + e.getMessage();           
            	rr.result = Result.ERROR;
            	return rr;
            } catch (RuntimeException e) {
            	rr.description = "Error during graph initialization: " +  e.getMessage();           
            	rr.result = Result.ERROR;
            	return rr;
            }
            
    		try {
    			result = futureResult.get();
    		} catch (InterruptedException e) {
    			rr.description = "Graph was unexpectedly interrupted !" + e.getMessage();            
            	rr.result = Result.ERROR;
            	return rr;
    		} catch (ExecutionException e) {
    			rr.description = "Error during graph processing !" + e.getMessage();            
            	rr.result = Result.ERROR;
            	return rr;
    		}
        } finally {
    		if (graph != null)
    			graph.free();
        }
		
        long totalTime = System.currentTimeMillis() - startTime;
        rr.result = result;
        rr.duration = totalTime;
        
		return rr;
	}
}
