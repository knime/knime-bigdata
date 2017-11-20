package org.knime.bigdata.spark.core.exception;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class InvalidConfigException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * @param message error message
     */
    public InvalidConfigException(final String message) {
        super(message);
    }
}
