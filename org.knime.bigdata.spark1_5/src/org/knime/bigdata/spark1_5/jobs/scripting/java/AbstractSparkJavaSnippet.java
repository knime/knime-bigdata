/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   25.11.2011 (hofer): created
 */
package org.knime.bigdata.spark1_5.jobs.scripting.java;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import org.knime.bigdata.spark.core.exception.InvalidSchemaException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark1_5.api.StructTypeBuilder;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public abstract class AbstractSparkJavaSnippet implements Serializable {

    private static final long serialVersionUID = -3509388164998408345L;

    private final static Logger LOGGER = Logger.getLogger(AbstractSparkJavaSnippet.class.getName());

    /**
     * Overwrite this method to return a manual create {@link StructType}.
     *
     * @param resultRDD the Spark RDD that contains the result
     * @return {@link StructType} that defines the column names and types
     * @throws InvalidSchemaException if the schema is invalid
     */
    protected StructType getSchema(final JavaRDD<Row> resultRDD) throws InvalidSchemaException {
        final List<Row> rowSample = resultRDD.take(10);
        if (rowSample.isEmpty()) {
            throw new InvalidSchemaException(
                "Snippet returned no rows. Cannot infer columns without having some rows.");
        }

        return StructTypeBuilder.fromRows(resultRDD.take(10)).build();
    }

    /**
     * @param sc the JavaSparkContext
     * @param rowRDD1 the first input RDD. Might be <code>null</code> if not connected.
     * @param rowRDD2 the second input RDD. Might be <code>null</code> if not connected.
     * @return the resulting RDD or <code>null</code>
     * @throws Exception if an exception occurs
     */
    public abstract JavaRDD<Row> apply(JavaSparkContext sc, JavaRDD<Row> rowRDD1, JavaRDD<Row> rowRDD2)
        throws Exception;

    /**
     * Write warning message to the logger.
     *
     * @param o The object to print.
     */
    protected void logWarn(final String o) {
        LOGGER.warning(o);
    }

    /**
     * Write debugging message to the logger.
     *
     * @param o The object to print.
     */
    protected void logDebug(final String o) {
        LOGGER.finest(o);
    }

    /**
     * Write info message to the logger.
     *
     * @param o The object to print.
     */
    protected void logInfo(final String o) {
        LOGGER.fine(o);
    }

    /**
     * Write error message to the logger.
     *
     * @param o The object to print.
     */
    protected void logError(final String o) {
        LOGGER.log(Level.SEVERE, o);
    }

    /**
     * Write warning message and throwable to the logger.
     *
     * @param o The object to print.
     * @param t The exception to log at debug level, including its stack trace.
     */
    protected void logWarn(final String o, final Throwable t) {
        LOGGER.log(Level.WARNING, o, t);
    }

    /**
     * Write debugging message and throwable to the logger.
     *
     * @param o The object to print.
     * @param t The exception to log, including its stack trace.
     */
    protected void logDebug(final String o, final Throwable t) {
        LOGGER.log(Level.FINEST, o, t);
    }

    /**
     * Write info message and throwable to the logger.
     *
     * @param o The object to print.
     * @param t The exception to log at debug level, including its stack trace.
     */
    protected void logInfo(final String o, final Throwable t) {
        LOGGER.log(Level.FINE, o, t);
    }

    /**
     * Write error message and throwable to the logger.
     *
     * @param o The object to print.
     * @param t The exception to log at debug level, including its stack trace.
     */
    protected void logError(final String o, final Throwable t) {
        LOGGER.log(Level.SEVERE, o, t);
    }
}
