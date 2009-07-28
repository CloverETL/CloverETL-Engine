
package org.jetel.data.formatter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.metadata.DataRecordMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public interface BatchPortDefinitionFormatter {

    /**
     * Initialization of data formatter by given metadata definition per port
     * 
     * @param _metadatas
     * @throws org.jetel.exception.ComponentNotReadyException
     */
    public void init(List<DataRecordMetadata> _metadatas) throws ComponentNotReadyException;

	public void reset();

    /**
     * Sets output data destination. Some of formatters allow to call this method repeatedly.
     * @param outputDataTarget
     */
    public void setDataTarget(Object outputDataTarget);

	/**
	 *  Closing/deinitialization of formatter
	 */
	public void close();


	/**
     * Formats data records wrapped by additional port information
     * based on provided metadata
     *
     * @param portDefinitions
     * @return
     * @throws java.io.IOException
     */
	public int write(Collection<PortDefinition> portDefinitions) throws IOException;


	/**
	 *  Formats header based on provided metadata
	 * @throws IOException
	 */
	public int writeHeader() throws IOException;


	/**
	 *  Formats footer based on provided metadata
	 * @throws IOException
	 */
	public int writeFooter() throws IOException;


	/**
	 *  Flush any unwritten data into output stream
	 * @throws IOException
	 */
	public void flush() throws IOException;

	/**
	 * This method writes all data (header, body, footer) which are to write, but doesn't close underlying streams.
	 *
	 * @throws IOException
	 */
	public void finish() throws IOException;
}
