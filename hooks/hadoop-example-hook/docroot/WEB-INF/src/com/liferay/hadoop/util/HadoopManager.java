/**
 * Copyright (c) 2000-2012 Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.hadoop.util;

import com.liferay.portal.kernel.exception.SystemException;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author Raymond Augé
 */
public class HadoopManager {

	public static FileSystem getFileSystem() throws SystemException {
		return getIstance()._getFileSystem();
	}

	public static HadoopManager getIstance() {
		return _instance;
	}

	private FileSystem _getFileSystem() throws SystemException {
		try {
			return FileSystem.get(_configuration);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	private HadoopManager() {

		// HDSF

		// TODO be more declarative!

		_configuration = new Configuration();

		_configuration.set("fs.default.name", "hdfs://localhost:54310");

		System.setProperty("HADOOP_USER_NAME", "hduser");
	}

	private static HadoopManager _instance = new HadoopManager();

	private Configuration _configuration;

}