package org.knime.bigdata.spark.core.util;

import java.io.IOException;
import java.nio.file.Path;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Supplier interface for temp files. Due to the Java 7 heritage in Spark/Hadoop, we are sometimes unable to use the
 * Java 8 supplier interface.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public interface TempFileSupplier {

    /**
     * Create a new temporary file.
     * 
     * @return path to the newly created temporary file.
     * 
     * @throws IOException If something went wrong while creating the temp file.
     */
    Path newTempFile() throws IOException;
}