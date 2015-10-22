package com.knime.bigdata.spark.jobserver.server.transformation;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.sql.Row;

/**
 * Interface for transformations of {@link Row}-valued {@link JavaRDD RDDs}.
 */
public interface AbstractTransformation extends Serializable {

    /**
     * @param input
     * @return transformed RDD
     */
    @Nonnull
    <T extends JavaRDDLike<Row, ?>> JavaRDD<Row> apply(@Nonnull T input);
}
