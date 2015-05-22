package com.knime.bigdata.spark.jobserver.client;

/**
 * enum that maps job-server job stati to Java
 *
 * @author dwk
 *
 */
public enum JobStatus {
    /**
     *
     */
	OK,
	/**
	 *
	 */
	RUNNING,
	/**
	 *
	 */
	DONE,
	/**
	 *
	 */
	GONE,
	/**
	 *
	 */
	UNKNOWN,
	/**
	 *
	 */
	ERROR,
	/**
	 *
	 */
	FINISHED;
}
