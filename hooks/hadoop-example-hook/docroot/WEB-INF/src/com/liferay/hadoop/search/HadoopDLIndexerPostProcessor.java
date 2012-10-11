/**
 * Copyright (c) 2000-2011 Liferay, Inc. All rights reserved.
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

package com.liferay.hadoop.search;

import com.liferay.hadoop.util.HadoopManager;
import com.liferay.portal.kernel.search.BooleanQuery;
import com.liferay.portal.kernel.search.Document;
import com.liferay.portal.kernel.search.IndexerPostProcessor;
import com.liferay.portal.kernel.search.SearchContext;
import com.liferay.portal.kernel.search.Summary;
import com.liferay.portal.kernel.util.StreamUtil;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portlet.documentlibrary.model.DLFileEntry;

import java.io.IOException;
import java.io.PrintWriter;

import java.util.Locale;

import javax.portlet.PortletURL;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Raymond Augé
 */
public class HadoopDLIndexerPostProcessor implements IndexerPostProcessor {

	public void postProcessContextQuery(BooleanQuery arg0, SearchContext arg1)
		throws Exception {

		// Not used
	}

	public void postProcessDocument(Document document, Object obj)
		throws Exception {

		DLFileEntry dlFileEntry = (DLFileEntry)obj;

		long companyId = dlFileEntry.getCompanyId();
		long repositoryId = dlFileEntry.getRepositoryId();

		String stringObject = document.toString();

		// remove JSON chars

		stringObject = StringUtil.replace(
			stringObject,
			new String[] {"\"", ",", ":", "{", "}", "[", "]"},
			new String[] {
				StringPool.SPACE, StringPool.SPACE, StringPool.SPACE,
				StringPool.SPACE, StringPool.SPACE, StringPool.SPACE,
				StringPool.SPACE
			});

		Path fullDirPath = HadoopManager.getFullDirPath(
			companyId, repositoryId, null);

		fullDirPath = new Path("/index".concat(fullDirPath.toString()));

		FSDataOutputStream outputStream = null;

		try {
			FileSystem fileSystem = HadoopManager.getFileSystem();

			String suffix = StringPool.SLASH.concat(document.getUID());

			outputStream = fileSystem.create(
				fullDirPath.suffix(suffix));

			PrintWriter pw = new PrintWriter(outputStream);

			pw.write(stringObject);

			pw.flush();
			pw.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			StreamUtil.cleanUp(outputStream);
		}
	}

	public void postProcessFullQuery(BooleanQuery arg0, SearchContext arg1)
		throws Exception {

		// Not used
	}

	public void postProcessSearchQuery(BooleanQuery arg0, SearchContext arg1)
		throws Exception {

		// Not used
	}

	public void postProcessSummary(
		Summary arg0, Document arg1, Locale arg2, String arg3,
		PortletURL arg4) {

		// Not used
	}

}