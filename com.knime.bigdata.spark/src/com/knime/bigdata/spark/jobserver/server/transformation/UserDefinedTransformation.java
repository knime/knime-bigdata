package com.knime.bigdata.spark.jobserver.server.transformation;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

/**
 * Abstract class for transformations of {@link Row}-valued {@link JavaRDD RDDs}.
 *
 * @author jfr
 */
public interface UserDefinedTransformation extends Serializable {

	@Nonnull
	<T extends JavaRDD<Row>> JavaRDD<Row> apply(@Nonnull T input);
}
