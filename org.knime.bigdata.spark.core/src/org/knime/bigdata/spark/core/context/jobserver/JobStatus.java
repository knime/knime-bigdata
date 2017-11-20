package org.knime.bigdata.spark.core.context.jobserver;

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
	FINISHED,

    /**
     *
     */
	KILLED;
}
