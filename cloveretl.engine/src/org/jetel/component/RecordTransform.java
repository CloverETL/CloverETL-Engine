/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.component;

import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Interface used by all components performing some sort of Reformat operation
 *
 *@author      dpavlis
 *@created     February 4, 2003
 *@since       April 18, 2002
 *@revision    $Revision$
 */
public interface RecordTransform {

	/**
	 *  Initializes reformat class/function
	 *
	 *@param  sourceMetadata  Metadata describing source data record
	 *@param  targetMetadata  Metadata describing target data record
	 *@return                 True if OK, otherwise False
	 *@since                  April 18, 2002
	 */
	public boolean init(DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata);


	/**
	 *  Initializes reformat class/function<br>
	 *  This method is mostly for joining data
	 *
	 *@param  sourcesMetadata  Metadata describing source data records [array]
	 *@param  targetMetadata   Metadata describing target data record
	 *@return                  True if OK, otherwise False
	 */
	public boolean init(DataRecordMetadata[] sourcesMetadata, DataRecordMetadata targetMetadata);


	/**
	 *  Perform reformat of one source record to target
	 *
	 *@param  source  Source DataRecord
	 *@param  target  Target DataRecord
	 *@return         True if OK, otherwise False
	 *@since          April 18, 2002
	 */
	public boolean transform(DataRecord source, DataRecord target);


	/**
	 *  Perform reformat of source records to one target record<br>
	 *  This method is mostly for joining data
	 *
	 *@param  sources  Source DataRecords
	 *@param  target   Target DataRecord
	 *@return          True if OK, otherwise False
	 */
	public boolean transform(DataRecord[] sources, DataRecord target);


	/**
	 *  Returns description of error if one of the methods failed
	 *
	 *@return    Error message
	 *@since     April 18, 2002
	 */
	public String getMessage();

}

