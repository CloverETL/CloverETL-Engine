/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-2003  David Pavlis
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
package org.jetel.data;

/**
 *  Helper class which contains some framework-wide constants defitions.<br>
 *  Change the compile-time defaults here !
 *
 *@author     dpavlis
 *@created    January 23, 2003
 */
public final class Defaults {

	/**
	 *  when buffering IO, what is the default size of buffer
	 */
	public final static int DEFAULT_INTERNAL_IO_BUFFER_SIZE = 32768;


	/**
	 *  Defaults regarding DataRecord structure/manipulation
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class Record {

		/**
		 *  Determines max size of record (serialized) in bytes
		 */
		public final static int MAX_RECORD_SIZE = 8192;
	}


	/**
	 *  Defaults for all DataParsers
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class DataParser {

		/**
		 *  max length of field's value representation (bytes or characters)
		 */
		public final static int FIELD_BUFFER_LENGTH = 512;

		/**
		 *  default decoder to be used if no specified
		 */
		public final static String DEFAULT_CHARSET_DECODER = "ISO-8859-1";
	}


	/**
	 *  Defaults for DataFormatters
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class DataFormatter {

		/**
		 *  default encoder to be used
		 */
		public final static String DEFAULT_CHARSET_ENCODER = "ISO-8859-1";
		/**
		 *  max length of field's value representation (bytes or characters)
		 */
		public final static int FIELD_BUFFER_LENGTH = 512;

	}


	/**
	 *  Defaults for various components
	 *
	 *@author     david
	 *@created    January 26, 2003
	 */
	public final static class Component {
		/**
		 *  Description of the Field
		 */
		public final static String KEY_FIELDS_DELIMITER_REGEX = "[:;|]";
	}


	/**
	 *  Defaults for section Data
	 *
	 *@author     dpavlis
	 *@created    6. duben 2003
	 */
	public final static class Data {
		/**
		 *  Unit size of data buffer which keeps data records for sorting/hashing
		 */
		public final static int DATA_RECORDS_BUFFER_SIZE = 10 * 1048576; // 10MB
		/**
		 *  How many units (buffers) can be allocated 
		 */
		public final static short MAX_BUFFERS_ALLOCATED = 99;
		// all together up to 990 MB
	}
}

