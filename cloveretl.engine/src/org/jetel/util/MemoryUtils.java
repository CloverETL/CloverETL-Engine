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
package org.jetel.util;

import java.lang.management.ManagementFactory;
import java.util.List;

import org.jetel.data.Defaults;

/**
 * Utility class for measuring memory footprint.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Sep 2011
 */
public class MemoryUtils {

	private static final String JVM_ARCH_PROPERTY = "sun.arch.data.model";
	private static final String FORCE_JVM_ARCH = "je.forceJVMArch";

	private static final int CACHE_ENTRY_OVERHEAD_32 = 72 + 8; // CacheEntry + hashed cache entry + map overhead
	private static final int CACHE_ENTRY_OVERHEAD_64 = 88 + 8;
	private static final int CACHE_ENTRY_OVERHEAD_OOPS = 88 + 8;

	private static final int BPAGE_OVERHEAD_32 = 64;
	private static final int BPAGE_OVERHEAD_64 = 76;
	private static final int BPAGE_OVERHEAD_OOPS = 76;

	private final static int ARRAY_OVERHEAD_32 = 16;
	private final static int ARRAY_OVERHEAD_64 = 24;
	private final static int ARRAY_OVERHEAD_OOPS = 16;

	private final static int ARRAY_SIZE_INCLUDED_32 = 4;
	private final static int ARRAY_SIZE_INCLUDED_64 = 0;
	private final static int ARRAY_SIZE_INCLUDED_OOPS = 0;

	private final static int OBJECT_ARRAY_ITEM_OVERHEAD_32 = 4;
	private final static int OBJECT_ARRAY_ITEM_OVERHEAD_64 = 8;
	private final static int OBJECT_ARRAY_ITEM_OVERHEAD_OOPS = 4;

	public static final int CACHE_ENTRY_OVERHEAD;
	public static final int BPAGE_OVERHEAD;
	public static final int ARRAY_OVERHEAD;
	public static final int ARRAY_SIZE_INCLUDED;
	public static final int OBJECT_ARRAY_ITEM_OVERHEAD;

	/* Primitive long array item size is the same on all platforms. */
	public final static int PRIMITIVE_LONG_ARRAY_ITEM_OVERHEAD = 8;

	static {
		boolean is64 = false;
		String overrideArch = System.getProperty(FORCE_JVM_ARCH);

		try {
			if (overrideArch == null) {
				String arch = System.getProperty(JVM_ARCH_PROPERTY);
				if (arch != null) {
					is64 = Integer.parseInt(arch) == 64;
				}
			} else {
				is64 = Integer.parseInt(overrideArch) == 64;
			}
		} catch (NumberFormatException NFE) {
			NFE.printStackTrace(System.err); // TODO:
		}

		boolean useCompressedOops = false;
		List<String> args = ManagementFactory.getRuntimeMXBean().getInputArguments();
		for (String arg : args) {
			if ("-XX:+UseCompressedOops".equals(arg)) {
				useCompressedOops = true;
				break;
			}
		}

		if (useCompressedOops) {
			CACHE_ENTRY_OVERHEAD = CACHE_ENTRY_OVERHEAD_OOPS;
			BPAGE_OVERHEAD = BPAGE_OVERHEAD_OOPS;
			ARRAY_OVERHEAD = ARRAY_OVERHEAD_OOPS;
			ARRAY_SIZE_INCLUDED = ARRAY_SIZE_INCLUDED_OOPS;
			OBJECT_ARRAY_ITEM_OVERHEAD = OBJECT_ARRAY_ITEM_OVERHEAD_OOPS;
		} else if (is64) {
			CACHE_ENTRY_OVERHEAD = CACHE_ENTRY_OVERHEAD_64;
			BPAGE_OVERHEAD = BPAGE_OVERHEAD_64;
			ARRAY_OVERHEAD = ARRAY_OVERHEAD_64;
			ARRAY_SIZE_INCLUDED = ARRAY_SIZE_INCLUDED_64;
			OBJECT_ARRAY_ITEM_OVERHEAD = OBJECT_ARRAY_ITEM_OVERHEAD_64;
		} else {
			CACHE_ENTRY_OVERHEAD = CACHE_ENTRY_OVERHEAD_32;
			BPAGE_OVERHEAD = BPAGE_OVERHEAD_32;
			ARRAY_OVERHEAD = ARRAY_OVERHEAD_32;
			ARRAY_SIZE_INCLUDED = ARRAY_SIZE_INCLUDED_32;
			OBJECT_ARRAY_ITEM_OVERHEAD = OBJECT_ARRAY_ITEM_OVERHEAD_32;
		}
	}

	/**
	 * Returns the memory size occupied by a byte array of a given length. All arrays (regardless of element type) have
	 * the same overhead for a zero length array. On 32b Java, there are 4 bytes included in that fixed overhead that
	 * can be used for the first N elements -- however many fit in 4 bytes. On 64b Java, there is no extra space
	 * included. In all cases, space is allocated in 8 byte chunks.
	 */
	public static int byteArraySize(int arrayLen) {

		/*
		 * ARRAY_OVERHEAD accounts for N bytes of data, which is 4 bytes on 32b Java and 0 bytes on 64b Java. Data
		 * larger than N bytes is allocated in 8 byte increments.
		 */
		int size = ARRAY_OVERHEAD;
		if (arrayLen > ARRAY_SIZE_INCLUDED) {
			size += ((arrayLen - ARRAY_SIZE_INCLUDED + 7) / 8) * 8;
		}

		return size;
	}

	public static int shortArraySize(int arrayLen) {
		return byteArraySize(arrayLen * 2);
	}

	public static int intArraySize(int arrayLen) {
		return byteArraySize(arrayLen * 4);
	}

	public static int longArraySize(int arrayLen) {
		return ARRAY_OVERHEAD + (arrayLen * 8);
	}

	public static int objectArraySize(int arrayLen) {
		return byteArraySize(arrayLen * OBJECT_ARRAY_ITEM_OVERHEAD);
	}

	/**
	 * @return size of available direct memory
	 * (currently this is just estimation based on size of heap, which equals to size of direct memory by default)
	 * @note should be changed in the future (https://bug.javlin.eu/browse/CL-2723)
	 * @see Defaults#CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE
	 */
	public static long getDirectMemorySize() {
		return Runtime.getRuntime().maxMemory();
	}
	
}
