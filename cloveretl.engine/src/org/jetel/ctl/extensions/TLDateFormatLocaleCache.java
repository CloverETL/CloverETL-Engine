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
package org.jetel.ctl.extensions;

import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.util.MiscUtils;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;

/**
 * @author jakub (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created May 25, 2010
 */
public class TLDateFormatLocaleCache extends TLCache {

	private DateFormatter cachedFormatter;
	private String previousLocaleString;
	
	public TLDateFormatLocaleCache(TLFunctionCallContext context, int pos1, int pos2) {
		createCachedLocaleFormat(context, pos1, pos2);
	}
	
	public void createCachedLocaleFormat(TLFunctionCallContext context, int pos1, int pos2) {
		
		if (context.getLiteralsSize() <= pos1)
			return;

		if (!(context.getParamValue(pos1) instanceof String))
			return;
		
		// this construction which allows overloading, where you can use the parameter on the same position as 'format' but 
		// in different meaning with overloaded functions (as long as it is not 'String')
		if (context.getLiteralsSize() <= pos2 || !(context.getParamValue(pos2) instanceof String)) {
			String paramPattern = (String)context.getParamValue(pos1);
			if (context.isLiteral(pos1)) {
				cachedFormatter = DateFormatterFactory.getFormatter(paramPattern);
			}
			return;
		}
		
		String paramPattern = (String)context.getParamValue(pos1);
		String paramLocale = (String)context.getParamValue(pos2);
		if (context.isLiteral(pos1) && context.isLiteral(pos2)) {
			cachedFormatter = DateFormatterFactory.getFormatter(paramPattern, MiscUtils.createLocale(paramLocale));
		}
	}

	
	public DateFormatter getCachedLocaleFormatter(TLFunctionCallContext context, String format, String locale,
			int pos1, int pos2) {
		
		if (context.getLiteralsSize() > Math.max(pos1, pos2)) {
			// if we use the variant with both format and locale specified
			if ((context.isLiteral(pos1) && context.isLiteral(pos2))
				// either both format and locale were literals (thus cached at init)
				|| (cachedFormatter != null 
						&& format.equals(cachedFormatter.getPattern()) 
						// careful when locale is null! See test_convertlib_date2str.ctl
						&& ((locale == previousLocaleString) || (locale != null && locale.equals(previousLocaleString)))
					)
				// or format is already cached and previous inputs match the current ones
				)
			{
				return cachedFormatter;
			} else {
				// otherwise we have to recompute cache and remember just in the case future input will be the same
				if (locale == null) {
					cachedFormatter = DateFormatterFactory.getFormatter(format);
					previousLocaleString = locale;
				} else {
					cachedFormatter = DateFormatterFactory.getFormatter(format, MiscUtils.createLocale(locale));
					previousLocaleString = locale;
				}

				return cachedFormatter;
			}
		}
		if (context.getLiteralsSize() > pos1 && context.getLiteralsSize() <= pos2) {
			// just format is specified, but not locale
			if (context.isLiteral(pos1) 
					|| (cachedFormatter != null && format.equals(cachedFormatter.getPattern()))
				) 
			{
				return cachedFormatter;
			} else {
				// same as above but just for format (default locale is used) 
				cachedFormatter = DateFormatterFactory.getFormatter(format);

				return cachedFormatter;				
			}
		}
		
		throw new TransformLangExecutorRuntimeException("Format not correctly specified for the date.");
		
	}
}
