/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkJavaSnippet extends KnimeSparkJob implements Serializable {

    //TODO: Provide a method to access the input structtype we could simply save the job configuration if
    //StructType is not serializable

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(AbstractSparkJavaSnippet.class.getName());

    private static final String PARAM_INPUT_TABLE_KEY1 = ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_INPUT_TABLE_KEY2 =  ParameterConstants.PARAM_TABLE_2;

    private static final String PARAM_OUTPUT_TABLE_KEY =  ParameterConstants.PARAM_TABLE_1;

    /**
     * parse parameters
     */
    @Override
    public final SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasOutputParameter(PARAM_OUTPUT_TABLE_KEY)) {
            msg = "Output parameter '" + PARAM_OUTPUT_TABLE_KEY + "' missing.";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY1) && !validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY1))) {
            msg = "(First) Input data table missing for key: " + aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY1);
        }
        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY2) && !validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2))) {
            msg = "Second input data table missing for key: " + aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2);
        }

        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client the primary result of this job should be a side
     * effect - new new RDD in the map of named RDDs
     *
     * @return JobResult with table information
     */
    @Override
    protected final JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.FINE, "starting transformation job...");
        final JavaRDD<Row> rowRDD1;
        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY1)) {
            rowRDD1 = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY1));
        } else {
            rowRDD1 = null;
        }
        final JavaRDD<Row> rowRDD2;
        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY2)) {
            rowRDD2 = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2));
        } else {
            rowRDD2 = null;
        }
        final JavaRDD<Row> resultRDD = apply(new JavaSparkContext(aSparkContext), rowRDD1, rowRDD2);
        LOGGER.log(Level.FINE, "transformation completed");
            if (resultRDD == null) {
                LOGGER.log(Level.FINE, "no result found");
                return JobResult.emptyJobResult().withMessage("OK");
            }
            try {
                LOGGER.log(Level.FINE, "result found");
                addToNamedRdds(aConfig.getOutputStringParameter(PARAM_OUTPUT_TABLE_KEY), resultRDD);
                final StructType schema = getSchema(resultRDD);
                return JobResult.emptyJobResult().withMessage("OK")
                        .withTable(aConfig.getOutputStringParameter(PARAM_OUTPUT_TABLE_KEY), schema);
            } catch (InvalidSchemaException e) {
                throw new GenericKnimeSparkException("Could not determine result schema. Error: " + e, e);
            }
    }

    /**
     * Overwrite this method to return a manual create {@link StructType}.
     * @param resultRDD the Spark RDD that contains the result
     * @return {@link StructType} that defines the column names and types
     * @throws InvalidSchemaException if the schema is invalid
     */
    protected StructType getSchema(final JavaRDD<Row> resultRDD) throws InvalidSchemaException {
        return StructTypeBuilder.fromRows(resultRDD.take(10)).build();
    }

    /**
     * @param sc the JavaSparkContext
     * @param rowRDD1 the first input RDD. Might be <code>null</code> if not connected.
     * @param rowRDD2 the second input RDD. Might be <code>null</code> if not connected.
     * @return the resulting RDD or <code>null</code>
     * @throws GenericKnimeSparkException if an exception occurs
     */
    public abstract JavaRDD<Row> apply(JavaSparkContext sc, JavaRDD<Row> rowRDD1, JavaRDD<Row> rowRDD2)
            throws GenericKnimeSparkException;

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
