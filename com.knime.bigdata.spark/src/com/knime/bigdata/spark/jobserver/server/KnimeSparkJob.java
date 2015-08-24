package com.knime.bigdata.spark.jobserver.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkContext;

import spark.jobserver.SparkJobValidation;

import com.typesafe.config.Config;

/**
 * handles translation of Scala interface to Java, wraps generic config with JobConfig
 *
 * @author dwk
 *
 */
public abstract class KnimeSparkJob extends KnimeSparkJobWithNamedRDD {

    /**
     * parameter name of single output input table (most jobs, but not all, use at least one input RDD)
     */
    public static final String PARAM_INPUT_TABLE = "InputTable";

    /**
     * parameter name of single output result table (most jobs, but not all, produce exactly one RDD)
     */
    public static final String PARAM_RESULT_TABLE = "ResultTable";

    final List<String[]> m_severe = new ArrayList<>();

    final List<String[]> m_warn = new ArrayList<>();

    private final Appender appender = new Appender() {

        @Override
        public void setName(final String name) {
        }

        @Override
        public void setLayout(final Layout layout) {
        }

        @Override
        public void setErrorHandler(final ErrorHandler errorHandler) {
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        public String getName() {
            return "KnimeJobLogger";
        }

        @Override
        public Layout getLayout() {
            return null;
        }

        @Override
        public Filter getFilter() {
            return null;
        }

        @Override
        public ErrorHandler getErrorHandler() {
            return null;
        }

        @Override
        public void doAppend(final LoggingEvent event) {
            if (event != null) {
                //System.err.println("logging request for: " + event.getLevel() + ": " + event.getMessage());

                if (m_severe.size() < 100 && event.getLevel() == Level.ERROR || event.getLevel() == Level.FATAL) {
                    m_severe.add(new String[] {event.getLoggerName() , event.getMessage().toString()});
                }
                if (m_warn.size() < 100 && event.getLevel() == Level.WARN) {
                    m_warn.add(new String[] {event.getLoggerName() , event.getMessage().toString()});
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void clearFilters() {
        }

        @Override
        public void addFilter(final Filter newFilter) {
        }
    };

    @Override
    public Object runJob(final Object aSparkContext, final Config aConfig) {
        final Logger ROOT_LOGGER = Logger.getRootLogger();
        ROOT_LOGGER.addAppender(appender);
        JobResult res;
        try {
            res = runJobWithContext((SparkContext)aSparkContext, new JobConfig(aConfig));
        } catch (Throwable t) {
            t.printStackTrace();
            res = JobResult.emptyJobResult().withMessage(t.getMessage()).withException(t);
        }
        res.addWarnings(m_warn);
        res.addErrors(m_severe);

        ROOT_LOGGER.removeAppender(appender);
        return res;
    }

    @Override
    public final SparkJobValidation validate(final Object aSparkContext, final Config aConfig) {
        return validate(new JobConfig(aConfig));
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
    public abstract SparkJobValidation validate(JobConfig aConfig);

    /**
     * run the actual job
     *
     * @param aSparkContext
     * @param aConfig
     * @return JobResult - a container for results as they are supported by KNIME
     * @throws GenericKnimeSparkException
     */
    protected abstract JobResult runJobWithContext(SparkContext aSparkContext, JobConfig aConfig)
        throws GenericKnimeSparkException;
}
