package com.knime.bigdata.spark.jobserver.server;

import org.apache.spark.SparkContext;

import spark.jobserver.SparkJobValidation;

import com.typesafe.config.Config;

/**
 * handles translation of Scala interface to Java
 * @author dwk
 *
 */
public abstract class KnimeSparkJob extends KnimeSparkJobWithNamedRDD {

	@Override
    public Object runJob(final Object aSparkContext, final Config aConfig) {
		return runJobWithContext((SparkContext) aSparkContext, aConfig);
	}

	@Override
    public SparkJobValidation validate(final Object aSparkContext, final Config aConfig) {
		return validateWithContext((SparkContext) aSparkContext, aConfig);
	}

	/**
	 * validate the configuration
	 * @param aSparkContext
	 * @param aConfig
	 * @return SparkJobValidation
	 */
	protected abstract SparkJobValidation validateWithContext(SparkContext aSparkContext,
			Config aConfig);

	/**
	 * run the actual job
	 * @param aSparkContext
	 * @param aConfig
	 * @return JobResult - a container for results as they are supported by KNIME
	 */
	protected abstract JobResult runJobWithContext(SparkContext aSparkContext, Config aConfig);
}
