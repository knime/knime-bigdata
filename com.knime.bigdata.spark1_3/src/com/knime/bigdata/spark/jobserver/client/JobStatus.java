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

	/**
	 * determine if given status represents an error
	 * @param aStatus
	 * @return true if given status is neither unknown nor error
	 */
	public static boolean isErrorStatus(final JobStatus aStatus) {
	    return aStatus == ERROR || aStatus == UNKNOWN;
	}
}
