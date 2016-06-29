/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.core.port.data;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 * This class represents a Spark SchemaRDD or data frame which represents a data table with columns and rows.
 * The table definition can be inspected with the {@link #getTableSpec()} method.
 * @author Tobias Koetter, KNIME.com
 */
public class SparkDataTable extends AbstractSparkRDD {

    private static final String TABLE_SPEC = "spec";

    private DataTableSpec m_spec;
    /**
     * @param contextID ID of the context the Spark data table lives in
     * @param spec the {@link DataTableSpec} of the Spark data table
     */
    public SparkDataTable(final SparkContextID contextID, final DataTableSpec spec) {
        this(contextID, SparkIDs.createRDDID(), spec);
    }

    /**
     * @param contextID the ID of context the Spark data table lives in
     * @param tableName the unique name of the Spark data table
     * @param spec the {@link DataTableSpec} of the Spark data table
     */
    public SparkDataTable(final SparkContextID contextID, final String tableName, final DataTableSpec spec) {
        super(contextID, tableName);
        if (spec == null) {
            throw new NullPointerException("spec must not be null");
        }
        m_spec = spec;
    }

    /**
     * @param in
     */
    public SparkDataTable(final ZipInputStream in) throws IOException {
        super(in);
        try {
            ZipEntry ze = in.getNextEntry();
            if (!ze.getName().equals(TABLE_SPEC)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                    + TABLE_SPEC + "\".");
            }
            final ModelContentRO specModel = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
                m_spec = DataTableSpec.load(specModel);
        } catch (InvalidSettingsException ise) {
            throw new IOException(ise);
        }
    }

    /**
     * @param out
     * @throws IOException
     */
    @Override
    public void save(final ZipOutputStream out) throws IOException {
        super.save(out);
        final ModelContent specModel = new ModelContent(TABLE_SPEC);
        m_spec.save(specModel);
        out.putNextEntry(new ZipEntry(TABLE_SPEC));
        specModel.saveToXML(new NonClosableOutputStream.Zip(out));
    }

    /**
     * @return the tableSpec
     */
    public DataTableSpec getTableSpec() {
        return m_spec;
    }
}
