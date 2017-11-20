package com.knime.bigdata.spark.core.exception;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author jfr
 */
@SparkClass
public class InvalidSchemaException extends KNIMESparkException {
    private static final long serialVersionUID = 1L;

    /**
     * @param message error message
     */
    public InvalidSchemaException(final String message) {
        super(message);
    }
}
