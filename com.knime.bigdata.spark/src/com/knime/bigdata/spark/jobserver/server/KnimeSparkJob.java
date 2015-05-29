package com.knime.bigdata.spark.jobserver.server;

import org.apache.spark.SparkContext;

import spark.jobserver.SparkJobValidation;

import com.typesafe.config.Config;

/**
 * handles translation of Scala interface to Java
 *
 * @author dwk
 *
 */
public abstract class KnimeSparkJob extends KnimeSparkJobWithNamedRDD {

    @Override
    public Object runJob(final Object aSparkContext, final Config aConfig) {
        try {
            return runJobWithContext((SparkContext)aSparkContext, aConfig);
        } catch (Throwable t) {
            return JobResult.emptyJobResult().withMessage(t.getMessage()).withException(t);
        }
    }

    @Override
    public final SparkJobValidation validate(final Object aSparkContext, final Config aConfig) {
        return validate(aConfig);
    }

    /**
     * validate the configuration
     *
     * note that this validation must be entirely based on the the configuration and must be executable on the client as
     * well as on the server
     *
     * @param aConfig
     * @return SparkJobValidation
     */
    public abstract SparkJobValidation validate(Config aConfig);

    /**
     * run the actual job
     *
     * @param aSparkContext
     * @param aConfig
     * @return JobResult - a container for results as they are supported by KNIME
     * @throws GenericKnimeSparkException
     */
    protected abstract JobResult runJobWithContext(SparkContext aSparkContext, Config aConfig)
        throws GenericKnimeSparkException;
}
