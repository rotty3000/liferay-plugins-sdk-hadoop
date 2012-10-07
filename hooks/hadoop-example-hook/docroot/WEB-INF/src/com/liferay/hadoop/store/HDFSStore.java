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

package com.liferay.hadoop.store;

import com.liferay.hadoop.util.HadoopManager;
import com.liferay.hadoop.util.StoreEvent;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portal.kernel.util.StreamUtil;
import com.liferay.portlet.documentlibrary.DuplicateFileException;
import com.liferay.portlet.documentlibrary.store.BaseStore;

import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * @author Raymond Augé
 */
public class HDFSStore extends BaseStore {

	@Override
	public void addDirectory(long companyId, long repositoryId, String dirName)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullDirPath(
			companyId, repositoryId, dirName);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			fileSystem.mkdirs(fullPath, FsPermission.getDefault());
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public void addFile(
			long companyId, long repositoryId, String fileName, InputStream is)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, VERSION_DEFAULT);

		FSDataOutputStream outputStream = null;

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			outputStream = fileSystem.create(fullPath);

			StreamUtil.transfer(is, outputStream, false);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
		finally {
			StreamUtil.cleanUp(outputStream);
		}
	}

	@Override
	public void checkRoot(long companyId) throws SystemException {
	}

	@Override
	public void deleteDirectory(
			long companyId, long repositoryId, String dirName)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullDirPath(
			companyId, repositoryId, dirName);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			fileSystem.delete(fullPath, true);

			Path parentPath = fullPath.getParent();

			deleteEmptyAncestors(parentPath);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public void deleteFile(
			long companyId, long repositoryId, String fileName)
		throws PortalException, SystemException {

		deleteFile(companyId, repositoryId, fileName, VERSION_DEFAULT);
	}

	@Override
	public void deleteFile(
			long companyId, long repositoryId, String fileName,
			String versionLabel)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, versionLabel);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			if (fileSystem.exists(fullPath)) {
				fileSystem.delete(fullPath, true);
			}

			Path parentPath = fullPath.getParent();

			deleteEmptyAncestors(companyId, repositoryId, parentPath);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public InputStream getFileAsStream(
			long companyId, long repositoryId, String fileName,
			String versionLabel)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, versionLabel);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			if (!fileSystem.exists(fullPath)) {
				throw new PortalException(
					"File " + fullPath.toUri().toString() + " does not exist");
			}

			return fileSystem.open(fullPath);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	public String[] getFileNames(long companyId, long repositoryId)
		throws SystemException {

		return getFileNames(companyId, repositoryId);
	}

	@Override
	public String[] getFileNames(
			long companyId, long repositoryId, String dirName)
		throws SystemException {

		Path fullPath = HadoopManager.getFullDirPath(
			companyId, repositoryId, dirName);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			FileStatus[] listStatus = fileSystem.listStatus(fullPath);

			if ((listStatus == null) || (listStatus.length < 1)) {
				return new String[0];
			}

			List<String> fileNameList = new ArrayList<String>(
				listStatus.length);

			for (FileStatus fileStatus : listStatus) {
				String fileStatusPathString = fileStatus.getPath().toString();

				int pos = fileStatusPathString.indexOf(dirName);

				if (pos != -1) {
					fileStatusPathString = fileStatusPathString.substring(pos);
				}

				fileNameList.add(fileStatusPathString);
			}

			return fileNameList.toArray(new String[fileNameList.size()]);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public long getFileSize(long companyId, long repositoryId, String fileName)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, VERSION_DEFAULT);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			if (!fileSystem.exists(fullPath)) {
				throw new PortalException(
					"File " + fullPath.toUri().toString() + " does not exist");
			}

			FileStatus fileStatus = fileSystem.getFileStatus(fullPath);

			return fileStatus.getLen();
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public boolean hasDirectory(
			long companyId, long repositoryId, String dirName)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullDirPath(
			companyId, repositoryId, dirName);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			return fileSystem.exists(fullPath);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public boolean hasFile(
			long companyId, long repositoryId, String fileName,
			String versionLabel)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, versionLabel);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			return fileSystem.exists(fullPath);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public void move(String srcDir, String destDir) throws SystemException {
	}

	@Override
	public void updateFile(
			long companyId, long repositoryId, long newRepositoryId,
			String fileName)
		throws PortalException, SystemException {

		Path sourcePath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, VERSION_DEFAULT);
		Path targetPath = HadoopManager.getFullVersionFilePath(
			companyId, newRepositoryId, fileName, VERSION_DEFAULT);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, sourcePath));

			if (fileSystem.exists(targetPath)) {
				throw new DuplicateFileException(fileName);
			}

			if (!fileSystem.exists(sourcePath)) {
				throw new PortalException(
					"File " + sourcePath.toUri().toString() + " does not exist");
			}

			boolean renamed = fileSystem.rename(sourcePath, targetPath);

			if (!renamed) {
				throw new SystemException(
					"File name directory was not renamed from " +
						sourcePath.toUri().toString() + " to " +
							targetPath.toUri().toString());
			}
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	public void updateFile(
			long companyId, long repositoryId, String fileName,
			String newFileName)
		throws PortalException, SystemException {

		Path sourcePath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, VERSION_DEFAULT);
		Path targetPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, newFileName, VERSION_DEFAULT);

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, sourcePath));

			if (fileSystem.exists(targetPath)) {
				throw new DuplicateFileException(fileName);
			}

			if (!fileSystem.exists(sourcePath)) {
				throw new PortalException(
					"File " + sourcePath.toUri().toString() + " does not exist");
			}

			boolean renamed = fileSystem.rename(sourcePath, targetPath);

			if (!renamed) {
				throw new SystemException(
					"File name directory was not renamed from " +
						sourcePath.toUri().toString() + " to " +
							targetPath.toUri().toString());
			}
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public void updateFile(
			long companyId, long repositoryId, String fileName,
			String versionLabel, InputStream inputStream)
		throws PortalException, SystemException {

		Path fullPath = HadoopManager.getFullVersionFilePath(
			companyId, repositoryId, fileName, versionLabel);

		FSDataOutputStream outputStream = null;

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, fullPath));

			outputStream = fileSystem.create(fullPath);

			StreamUtil.transfer(inputStream, outputStream, false);
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
		finally {
			StreamUtil.cleanUp(outputStream);
		}
	}

	protected void deleteEmptyAncestors(Path path) throws SystemException {
		deleteEmptyAncestors(-1, -1, path);
	}

	protected void deleteEmptyAncestors(
		long companyId, long repositoryId, Path path) throws SystemException {

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem(
				new StoreEvent(companyId, repositoryId, path));

			FileStatus[] listStatus = fileSystem.listStatus(path);

			if ((listStatus == null) || (listStatus.length > 0)) {
				return;
			}

			Path parentPath = path.getParent();

			if (fileSystem.delete(path, true) &&
				fileSystem.exists(parentPath)) {

				deleteEmptyAncestors(companyId, repositoryId, parentPath);
			}
		}
		catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

}