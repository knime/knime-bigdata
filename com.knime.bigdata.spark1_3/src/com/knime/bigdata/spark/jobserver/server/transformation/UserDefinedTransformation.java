package com.knime.bigdata.spark.jobserver.server.transformation;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

/**
 * Abstract class for transformations of {@link Row}-valued {@link JavaRDD RDDs}.
 *
 * @author jfr
 */
public interface UserDefinedTransformation extends Serializable {

	/**
	 * @param aInput1
	 * @param aInput2
	 * @return transformation result
	 */
	@Nonnull
	<T extends JavaRDD<Row>> JavaRDD<Row> apply(@Nonnull T aInput1, T aInput2);
}
