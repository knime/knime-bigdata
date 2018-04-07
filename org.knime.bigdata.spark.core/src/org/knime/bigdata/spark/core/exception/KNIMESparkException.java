package org.knime.bigdata.spark.core.exception;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * This class shall be used to indicate failures in the KNIME Extension for Apache Spark. The message of this exception
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

    /**Standard see log message shown in exceptions.*/
    public static final String SEE_LOG_SNIPPET = "For details see View > Open KNIME log.";

    /** Optional stack trace of original exception. */
    private final String m_stackTrace;

    /**
     * Constructor for cases where an instructive error message can be reported.
     *
     * @param message An explanation or instruction that a user can act upon, e.g. change a setting, reset all nodes.
     */
    public KNIMESparkException(final String message) {
        this(message, null);
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
        super(message);
        m_stackTrace = toStringStackTrace(cause);
    }

    /**
     * Constructor for cases where /NO/ instructive error message exists, because it is an internal error and/or a bug.
     * In this case the resulting {@link KNIMESparkException} will recurse through the given cause-hierarchy, trying to
     * extract the deepest non-blank error message. If there is no such thing, a generic message will be set.
     *
     * @param cause The original exception that caused the error. You can assume that this exception will be logged
     *            automatically and will show up in the KNIME log.
     */
    public KNIMESparkException(final Throwable cause) {
        this(buildMessage(cause), cause);
    }

    private static String buildMessage(final Throwable cause) {
        final String deepestErrorMsg = getDeepestErrorMessage(cause, true);

        if (deepestErrorMsg != null) {
            return deepestErrorMsg;
        } else {
            return "An error occured. " + SEE_LOG_SNIPPET;
        }
    }

    /** @return Stack trace of given cause as string. */
    private String toStringStackTrace(final Throwable cause) {
        if (cause == null) {
            return null;
        } else {
            try(final StringWriter sw = new StringWriter(); final PrintWriter pw = new PrintWriter(sw);) {
                cause.printStackTrace(pw);
                return sw.toString();
            } catch (IOException e) {
                return "Unable to print stack trace for Throwable: " + cause.getClass().getName()
                        + ". Exception: " + e.getMessage();
            }
        }
    }

    /**
     * Returns deepest non empty error message from the given exception and its cause stack.
     *
     * @param t A throwable, possibly with cause chain.
     * @param appendType Whether to append the type of the deepest exception with non-empty error message to the
     *            returned string.
     * @return deepest non empty error message or null.
     */
    public static String getDeepestErrorMessage(final Throwable t, final boolean appendType) {
        String deeperMsg = null;
        if (t.getCause() != null) {
            deeperMsg = getDeepestErrorMessage(t.getCause(), appendType);
        }

        if (deeperMsg != null && deeperMsg.length() > 0) {
            return deeperMsg;
        } else if (t.getMessage() != null && t.getMessage().length() > 0) {
            if (appendType) {
                return String.format("%s (%s)", t.getMessage(), t.getClass().getSimpleName());
            } else {
                return t.getMessage();
            }
        } else {
            return null;
        }
    }

    @Override
    public void printStackTrace(final PrintStream s) {
        if (m_stackTrace != null) {
            s.append(m_stackTrace);
        } else {
            super.printStackTrace(s);
        }
    }

    @Override
    public void printStackTrace(final PrintWriter s) {
        if (m_stackTrace != null) {
            s.append(m_stackTrace);
        } else {
            super.printStackTrace(s);
        }
    }
}
