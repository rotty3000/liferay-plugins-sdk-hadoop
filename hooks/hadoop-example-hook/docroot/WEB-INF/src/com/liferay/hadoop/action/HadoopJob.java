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

package com.liferay.hadoop.action;

import com.liferay.hadoop.job.Map;
import com.liferay.hadoop.job.Reduce;
import com.liferay.hadoop.util.HadoopManager;
import com.liferay.portal.kernel.struts.StrutsAction;
import com.liferay.portal.kernel.util.ContentTypes;
import com.liferay.portal.kernel.util.StreamUtil;

import java.io.InputStream;
import java.io.PrintWriter;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * <a href="HadoopJob.java.html"><b><i>View Source</i></b></a>
 *
 * @author Raymond Augé
 */
public class HadoopJob implements StrutsAction {

	public String execute(
			HttpServletRequest request, HttpServletResponse response)
		throws Exception {

		return doExecute(request, response);
	}

	public String execute(
			StrutsAction strutsAction, HttpServletRequest request,
			HttpServletResponse response)
		throws Exception {

		return doExecute(request, response);
	}

	public String doExecute(
			HttpServletRequest request, HttpServletResponse response)
		throws Exception {

		response.setContentType(ContentTypes.TEXT_PLAIN_UTF8);

		PrintWriter writer = response.getWriter();

		FileSystem fileSystem = HadoopManager.getFileSystem();

		JobClient jobClient = HadoopManager.getJobClient();

		writer.println("-- Job Status --");

		Path inputPath = new Path("/index/*/*");
		Path outputPath = new Path("/wordcount/results");

		try {
			if (_runningJob == null) {
				writer.println("Creating job");

				if (fileSystem.exists(_jobPath)) {
					fileSystem.delete(_jobPath, false);
				}

				if (!fileSystem.exists(_jobPath)) {
					writer.println("Deploying the job code to cluster");

					FSDataOutputStream outputStream = null;

					try {
						outputStream = fileSystem.create(_jobPath);

						ServletContext servletContext =
							HadoopManager.getServletContext();

						InputStream inputStream =
							servletContext.getResourceAsStream(
								"/WEB-INF/lib/hadoop-job.jar");

						StreamUtil.transfer(inputStream, outputStream, false);
					}
					finally {
						StreamUtil.cleanUp(outputStream);
					}

					writer.println("Job code deployed to cluster");
				}

				if (fileSystem.exists(outputPath)) {
					writer.println("A previous job output was found, backing it up");

					fileSystem.rename(
						outputPath, outputPath.getParent().suffix(
							"/.results-" + System.currentTimeMillis()));
				}

				_jobConf = HadoopManager.createNewJobConf();

				_jobConf.setJobName("Word Count");

				writer.println("Job '" + _jobConf.getJobName() + "' is being configured");

				_jobConf.setJarByClass(Map.class);
				_jobConf.setOutputKeyClass(Text.class);
				_jobConf.setOutputValueClass(IntWritable.class);
				_jobConf.setMapperClass(Map.class);
				_jobConf.setCombinerClass(Reduce.class);
				_jobConf.setReducerClass(Reduce.class);
				_jobConf.setInputFormat(TextInputFormat.class);
				_jobConf.setOutputFormat(TextOutputFormat.class);

				writer.println("Job code deployed to distributed cache's classpath");

				DistributedCache.addArchiveToClassPath(
					_jobPath, _jobConf, fileSystem);

				FileInputFormat.setInputPaths(_jobConf, inputPath);
				FileOutputFormat.setOutputPath(_jobConf, outputPath);

				writer.println("Submitting job the first time");

				_runningJob = jobClient.submitJob(_jobConf);

				writer.println("Job submitted");
			}

			int jobState = _runningJob.getJobState();

			writer.println("Job status: " + jobState + " (RUNNING = 1, SUCCEEDED = 2, FAILED = 3, PREP = 4, KILLED = 5)");

			if ((jobState != JobStatus.RUNNING) &&
				(jobState != JobStatus.PREP)) {

				writer.println("Re-issuing the job");

				if (fileSystem.exists(outputPath)) {
					writer.println("A previous job output was found, backing it up");

					fileSystem.rename(
						outputPath, outputPath.getParent().suffix(
							"/.results-" + System.currentTimeMillis()));
				}

				writer.println("Submitting job the first time");

				_runningJob = jobClient.submitJob(_jobConf);

				writer.println("Job submitted");
			}
		}
		catch (Exception ioe) {
			writer.println("Job error: ");

			ioe.printStackTrace(writer);
		}

		writer.flush();
		writer.close();

		return null;
	}

	private JobConf _jobConf;
	private Path _jobPath = new Path("/wordcount/jars/hadoop-job.jar");
	private RunningJob _runningJob;

}