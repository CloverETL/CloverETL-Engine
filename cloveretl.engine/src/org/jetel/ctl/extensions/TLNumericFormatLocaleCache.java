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

import java.util.Locale;

import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;

/**
 * @author javlin (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created May 28, 2010
 */
public class TLNumericFormatLocaleCache extends TLFormatterCache {
	
	private NumericFormatter cachedFormatter;
	private String previousFormatString;
	private boolean isDecimal;
	
	/**
	 * This cache can work in two modes.
	 * 1) For isDecimal equal false 'NumericFormatterFactory.createFormatter()' is used to create formatter.
	 * 2) For isDecimal equal true 'NumericFormatterFactory.createDecimalFormatter()' is used to create formatter.
	 * @param isDecimal 
	 */
	public TLNumericFormatLocaleCache(TLFunctionCallContext context, boolean isDecimal) {
		super(context);
		this.isDecimal = isDecimal;
	}

	public TLNumericFormatLocaleCache(TLFunctionCallContext context) {
		this(context, false);
	}

	public void createCachedLocaleFormat(TLFunctionCallContext context, int pos1, int pos2) {

		if (context.getLiteralsSize() <= pos1)
			return;

		if (!(context.getParamValue(pos1) instanceof String))
			return;

		if (context.getLiteralsSize() <= pos2 || !(context.getParamValue(pos2) instanceof String)) {
			String paramPattern = (String) context.getParamValue(pos1);
			if (context.isLiteral(pos1)) {
				prepareCachedFormatter(context, paramPattern, context.getDefaultLocale());
			}
			return;
		}

		String paramPattern = (String) context.getParamValue(pos1);
		String paramLocale = (String) context.getParamValue(pos2);
		if (context.isLiteral(pos1) && context.isLiteral(pos2)) {
			prepareCachedFormatter(context, paramPattern, getLocale(paramLocale));
		}
	}
	
	public NumericFormatter getCachedLocaleFormat(TLFunctionCallContext context, String format, String locale, int pos1, int pos2) {

		if (context.getLiteralsSize() > Math.max(pos1, pos2)) {
			// if we use the variant with both format and locale specified
			if (cachedFormatter != null && 
					((context.isLiteral(pos1) && context.isLiteral(pos2))
					// either both format and locale were literals (thus cached at init)
					|| (cachedFormatter != null && format.equals(previousFormatString) && locale.equals(previousLocaleString))
					// or format is already cached and previous inputs match the current ones
			)) {
				return cachedFormatter;
			} else {
				// otherwise we have to recompute cache and remember just in the case future input will be the same
				prepareCachedFormatter(context, format, getLocale(locale));
				previousFormatString = format;
				return cachedFormatter;
			}
		}
		if (context.getLiteralsSize() > pos1 && context.getLiteralsSize() <= pos2) {
			// just format is specified, but not locale
			if (cachedFormatter != null &&
					(context.isLiteral(pos1) || (cachedFormatter != null && format.equals(previousFormatString)))) {
				return cachedFormatter;
			} else {
				// same as above but just for format (default locale is used)
				prepareCachedFormatter(context, format, null);
				previousFormatString = format;
				return cachedFormatter;
			}
		}
		throw new TransformLangExecutorRuntimeException("Format not correctly specified for the number.");
	}
	
	private void prepareCachedFormatter(TLFunctionCallContext context, String format, Locale locale) {
		if (!isDecimal) {
			cachedFormatter = NumericFormatterFactory.getFormatter(format, locale, context.getDefaultLocale()); 
		} else {
			cachedFormatter = NumericFormatterFactory.getDecimalFormatter(format, locale, context.getDefaultLocale()); 
		}
	}
	
	public void setIsDecimal(boolean isDecimal) {
		if (this.isDecimal != isDecimal) {
			this.isDecimal = isDecimal;
			cachedFormatter = null;
		}
	}

	/*
	 * For <code>null<code> input, returns <code>null<code>.
	 * We need to preserve the information that the user has not specified any locale at all.
	 */
	@Override
	protected Locale getLocale(String localeString) {
		// NumericFormatterFactory.createNumberFormatter(String, Locale) checks if Locale is null
		if (localeString == null) { 
			return null;
		}
		return super.getLocale(localeString);
	}
}
