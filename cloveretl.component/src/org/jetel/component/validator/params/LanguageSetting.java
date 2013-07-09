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
package org.jetel.component.validator.params;

import java.util.Calendar;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.rules.LanguageSettingsValidationRule;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.data.Defaults;

/**
 * Small wrapper object which contains language setting (formatting masks, locale, timezone) used by validation rules.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 11.4.2013
 * @see LanguageSettingsValidationRule
 * @see ValidationGroup
 * @see ValidationNode
 */
@XmlRootElement(name="languageSetting")
public class LanguageSetting {
	public LanguageSetting() {
		this(false);
	}
	/**
	 * Creates new language setting and 
	 * @param defaults
	 */
	public LanguageSetting(boolean defaults) {
		if(defaults) {
			dateFormat.setValue(CommonFormats.defaultDate);
			numberFormat.setValue(CommonFormats.defaultNumber);
			locale.setValue(Defaults.DEFAULT_LOCALE);
			timezone.setValue(Calendar.getInstance().getTimeZone().getID());
		}
	}
	
	@XmlAttribute(name="dateFormat",required=false)
	private StringEnumValidationParamNode dateFormat = new StringEnumValidationParamNode();

	@XmlAttribute(name="numberFormat",required=false)
	private StringEnumValidationParamNode numberFormat = new StringEnumValidationParamNode();
	
	@XmlAttribute(name="locale",required=false)
	private StringEnumValidationParamNode locale = new StringEnumValidationParamNode();
	
	@XmlAttribute(name="timezone",required=false)
	private StringEnumValidationParamNode timezone = new StringEnumValidationParamNode();
	
	@Override
	public String toString() {
		return "Date format=" + dateFormat.getValue() + ",Number format=" + numberFormat.getValue() + ",Locale=" + locale.getValue() + ",Timezone=" + timezone.getValue(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	public void initialize() {
		dateFormat.setName(ValidatorMessages.getString("LanguageSetting.DateFormatParameterName")); //$NON-NLS-1$
		dateFormat.setPlaceholder(ValidatorMessages.getString("LanguageSetting.DateFormatPlaceholder")); //$NON-NLS-1$
		dateFormat.setOptions(CommonFormats.dates);
		numberFormat.setName(ValidatorMessages.getString("LanguageSetting.NumberFormatParameterName")); //$NON-NLS-1$
		numberFormat.setPlaceholder(ValidatorMessages.getString("LanguageSetting.NumberFormatPlaceholder")); //$NON-NLS-1$
		numberFormat.setOptions(CommonFormats.numbers);
		locale.setName(ValidatorMessages.getString("LanguageSetting.LocaleParameterName")); //$NON-NLS-1$
		locale.setPlaceholder(ValidatorMessages.getString("LanguageSetting.LocalePlaceholder")); //$NON-NLS-1$
		locale.setOptions(CommonFormats.locales);
		locale.setTooltip(ValidatorMessages.getString("LanguageSetting.LocaleTooltip")); //$NON-NLS-1$
		timezone.setName(ValidatorMessages.getString("LanguageSetting.TimezoneParameterName")); //$NON-NLS-1$
		timezone.setPlaceholder(ValidatorMessages.getString("LanguageSetting.TimezonePlaceholder")); //$NON-NLS-1$
		timezone.setOptions(CommonFormats.timezones);
		timezone.setTooltip(ValidatorMessages.getString("LanguageSetting.TimezoneTooltip")); //$NON-NLS-1$
	}
	
	public StringEnumValidationParamNode getDateFormat() {
		return dateFormat;
	}
	
	public StringEnumValidationParamNode getNumberFormat() {
		return numberFormat;
	}
	
	public StringEnumValidationParamNode getLocale() {
		return locale;
	}
	
	public StringEnumValidationParamNode getTimezone() {
		return timezone;
	}
	
	/**
	 * Merge two language settings hierarchically.
	 * @param current Current language settings
	 * @param parent Parent language setting - lower priority, take into account only when empty in current.
	 * @return New instance of language setting merged according to hierarchy
	 */
	public static LanguageSetting hierarchicMerge(LanguageSetting current, LanguageSetting parent) {
		LanguageSetting output = new LanguageSetting();
		if(current == null) {
			output.dateFormat.setValue(parent.dateFormat.getValue());
			output.numberFormat.setValue(parent.numberFormat.getValue());
			output.locale.setValue(parent.locale.getValue());
			output.timezone.setValue(parent.timezone.getValue());
			return output;
		}
		if(parent == null) {
			output.dateFormat.setValue(current.dateFormat.getValue());
			output.numberFormat.setValue(current.numberFormat.getValue());
			output.locale.setValue(current.locale.getValue());
			output.timezone.setValue(current.timezone.getValue());
			return output;
		}
		if(current.dateFormat.getValue().isEmpty()) {
			output.dateFormat.setValue(parent.dateFormat.getValue());
		} else {
			output.dateFormat.setValue(current.dateFormat.getValue());
		}
		if(current.numberFormat.getValue().isEmpty()) {
			output.numberFormat.setValue(parent.numberFormat.getValue());
		} else {
			output.numberFormat.setValue(current.numberFormat.getValue());
		}
		if(current.locale.getValue().isEmpty()) {
			output.locale.setValue(parent.locale.getValue());
		} else {
			output.locale.setValue(current.locale.getValue());
		}
		if(current.timezone.getValue().isEmpty()) {
			output.timezone.setValue(parent.timezone.getValue());
		} else {
			output.timezone.setValue(current.timezone.getValue());
		}
		return output;
	}
	
}
