package org.knime.bigdata.spark.core.sparkjobserver.context;

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
