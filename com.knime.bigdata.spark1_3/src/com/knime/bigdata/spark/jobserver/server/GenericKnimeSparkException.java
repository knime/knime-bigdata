package com.knime.bigdata.spark.jobserver.server;

import javax.annotation.Nonnull;

/**
 *
 * TODO - decide what to do with these....
 *
 * @author dwk
 *
 */
public class GenericKnimeSparkException extends Exception {
	private static final long serialVersionUID = 1L;

	/**
	 * error message prefix
	 */
	public static final String ERROR = "ERROR";

	/**
	 *
	 * @param cause
	 */
	public GenericKnimeSparkException(@Nonnull final Exception cause) {
		super(cause);
	}

	/**
	 *
	 * @param message
	 */
	public GenericKnimeSparkException(@Nonnull final String message) {
		super(message);
	}

	/**
	 *
	 * @param message
	 * @param cause
	 */
	public GenericKnimeSparkException(@Nonnull final String message,
			@Nonnull final Exception cause) {
		super(message, cause);
	}
}
