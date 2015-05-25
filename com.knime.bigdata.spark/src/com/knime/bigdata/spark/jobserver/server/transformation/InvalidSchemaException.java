package com.knime.bigdata.spark.jobserver.server.transformation;

public class InvalidSchemaException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidSchemaException(final String message) {
    super(message);
  }
}
