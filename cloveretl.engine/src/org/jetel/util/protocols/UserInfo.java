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
package org.jetel.util.protocols;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;

import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.exception.JetelRuntimeException;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 15, 2012
 */
public class UserInfo {

	private final String userInfo;
	private final String user;
	private final String password;
	
	public UserInfo(String userInfo) {
		this.userInfo = userInfo;
		String tmpUser = null;
		String tmpPassword = null;
		if (userInfo != null) {
			String[] tmp = userInfo.split(":");
			if (tmp.length > 1) {
				tmpUser = tmp[0];
				tmpPassword = tmp[1];
			} else if (tmp.length > 0) {
				tmpUser = tmp[0];
			}
		}
		user = tmpUser;
		password = tmpPassword;
	}

	public UserInfo(String user, String password) {
		this.user = user;
		this.password = password;
		String tmpUserInfo = null;
		if (user != null || password != null) {
			StringBuilder sb = new StringBuilder();
			if (user != null) {
				sb.append(user);
			}
			sb.append(":");
			if (password != null) {
				sb.append(password);
			}
			tmpUserInfo = sb.toString();
		}
		this.userInfo = tmpUserInfo;
	}
	
	public String getUserInfo() {
		return userInfo;
	}
	public String getUser() {
		return user;
	}
	public String getPassword() {
		return password;
	}
	
	public boolean isEmpty() {
		return (user == null) && (password == null);
	}

	@Override
	public String toString() {
		return getUserInfo();
	}

	/**
	 * @return {@link UserInfo} instance based on the given {@link Authority},
	 * individual parts of UserInfo are decoded
	 */
	public static UserInfo fromAuthority(Authority authority) {
		return fromString(authority.getUserInfo());
	}

	/**
	 * @return {@link UserInfo} instance based on the given {@link URI},
	 * individual parts of UserInfo are decoded
	 */
	public static UserInfo fromURI(URI uri) {
		return fromString(uri.getRawUserInfo());
	}

	/**
	 * @return {@link UserInfo} instance based on the given {@link URL},
	 * individual parts of UserInfo are decoded
	 */
	public static UserInfo fromURL(URL url) {
		return fromString(url.getUserInfo());
	}

	/**
	 * @return {@link UserInfo} instance based on the given user info string,
	 * individual parts of UserInfo are decoded
	 */
	public static UserInfo fromString(String userInfo) {
		if (userInfo == null) {
			return new UserInfo("", null);
		}
		String[] userInfoParts = userInfo.split(":");
		if (userInfoParts.length > 2) {
			throw new JetelRuntimeException("Invalid user information: " + userInfo);
		}
		if (userInfoParts.length == 0) {
			return new UserInfo("", null);
		} else if (userInfoParts.length == 1) {
			return new UserInfo(decodeUserInfoPart(userInfoParts[0]), null);
		} else {
			return new UserInfo(decodeUserInfoPart(userInfoParts[0]), decodeUserInfoPart(userInfoParts[1]));
		}
	}
	
	private static String decodeUserInfoPart(String s) {
		try {
			return URLDecoder.decode(s, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}
	
}
