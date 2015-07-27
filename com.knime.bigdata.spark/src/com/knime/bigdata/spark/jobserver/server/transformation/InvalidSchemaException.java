package com.knime.bigdata.spark.jobserver.server.transformation;

/**
 *
 * @author jfr
 */
public class InvalidSchemaException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * @param message error message
     */
    public InvalidSchemaException(final String message) {
        super(message);
    }
}
