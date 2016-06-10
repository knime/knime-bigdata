package com.knime.bigdata.spark.core.exception;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * This class shall be used to indicate failures in the KNIME Spark Executor extension. The message of this exception
 * class will be presented to a user e.g. in the tooltip text of a failed node in KNIME Analytics Platform. Therefore,
 * the message should provide an explanation or instruction that a user can act upon, e.g. change a setting, reset all
 * nodes. In those cases where this is not possible, e.g. if there was a NullPointerException, a class
 * cast failed, etc, you can use the {@link #KNIMESparkException(Exception)} constructor that will result in a
 * boilerplate message being shown to the user.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class KNIMESparkException extends Exception {

    private static final long serialVersionUID = 1L;

    public static final String SEE_LOG_SNIPPET = "(for details see View > Open KNIME log)";

    /**
     * Constructor for cases where an instructive error message can be reported.
     *
     * @param message An explanation or instruction that a user can act upon, e.g. change a setting, reset all nodes.
     */
    public KNIMESparkException(final String message) {
        super(message);
    }

    /**
     * Constructor for cases where an instructive error message can be reported and there is an underlying Exception
     * that should be logged.
     *
     * @param message An explanation or instruction that a user can act upon, e.g. change a setting, reset all nodes.
     * @param cause The original exception that caused the error. You can assume that this exception will be logged
     *            automatically and will show up in the KNIME log.
     */
    public KNIMESparkException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor for cases where /NO/ instructive error message exists, because it is an internal error and/or a bug.
     * In this case the constructed {@link KNIMESparkException} will have a generic message that points the user to the
     * log file.
     *
     * @param cause The original exception that caused the error. You can assume that this exception will be logged
     *            automatically and will show up in the KNIME log.
     */
    public KNIMESparkException(final Throwable cause) {
        super(String.format("%s %s",
            (cause.getMessage() == null) ? "An error occured" : "An error occured: " + cause.getMessage(), SEE_LOG_SNIPPET), cause);
    }
}