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

package com.liferay.hadoop.job;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Raymond Augé
 */
public class Map implements Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(
			LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());

			output.collect(word, one);
		}
	}

	public void configure(JobConf job) {
	}

	public void close() throws IOException {
	}

	private final static IntWritable one = new IntWritable(1);

	private Text word = new Text();

}