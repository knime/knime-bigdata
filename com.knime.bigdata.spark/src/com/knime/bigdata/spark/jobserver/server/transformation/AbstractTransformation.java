package com.knime.bigdata.spark.jobserver.server.transformation;


import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.sql.api.java.Row;


/**
 * Interfaced for transformations of {@link Row}-valued {@link JavaRDD RDDs}.
 */
public interface AbstractTransformation extends Serializable {
  @Nonnull
  <T extends JavaRDDLike<Row, ?>> JavaRDD<Row> apply(@Nonnull T input);
}
