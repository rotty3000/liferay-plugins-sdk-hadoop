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

import org.apache.hadoop.fs.Path;

/**
 * @author Raymond Augé
 */
public class StoreEvent {

	public StoreEvent(long companyId, long repositoryId, Path path) {
		_companyId = companyId;
		_path = path;
		_repositoryId = repositoryId;
	}

	public long getCompanyId() {
		return _companyId;
	}

	public Path getPath() {
		return _path;
	}

	public long getRepositoryId() {
		return _repositoryId;
	}

	public Path getRootPath() {
		return HadoopManager.getFullDirPath(
			getCompanyId(), getRepositoryId(), null);
	}

	private long _companyId;
	private Path _path;
	private long _repositoryId;

}