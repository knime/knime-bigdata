/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.core.port.data;

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

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.util.SparkIDs;

/**
 * This class represents tabular data in Spark, which has columns, rows and a {@link DataTableSpec}. In practice this is
 * a Spark SchemaRDD or data frame, depending on the Spark version.
 *
 * The table spec can be inspected with the {@link #getTableSpec()} method.
 *
 * Instances of this class are immutable.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkDataTable extends DefaultSparkData {

    private static final String TABLE_SPEC = "spec";

    private final DataTableSpec m_spec;

    /**
     * Creates a new Spark data table instance.
     *
     * @param contextID The ID of the context the Spark data table lives in.
     * @param spec The {@link DataTableSpec} of the Spark data table.
     */
    public SparkDataTable(final SparkContextID contextID, final DataTableSpec spec) {
        this(contextID, SparkIDs.createSparkDataObjectID(), spec);
    }

    /**
     * Creates a new Spark data table instance.
     *
     * @param contextID The ID of context the Spark data table lives in.
     * @param id The unique id of the data object in Spark (e.g. a UUID).
     * @param spec The {@link DataTableSpec} of the Spark data table.
     */
    public SparkDataTable(final SparkContextID contextID, final String id, final DataTableSpec spec) {

        super(contextID, id);
        if (spec == null) {
            throw new NullPointerException("spec must not be null");
        }
        m_spec = spec;
    }

    /**
     * Initializes a new instance from the given {@link ZipInputStream}.
     *
     * @param in A {@link ZipInputStream} to initialize this instance from.
     * @throws IOException when something goes wrong during initialization.
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
     * Saves this Spark data table to the given output stream.
     *
     * @param out An output stream to save to.
     * @throws IOException when something goes wrong during saving.
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
     * @return the {@link DataTableSpec} of the Spark data table.
     */
    public DataTableSpec getTableSpec() {
        return m_spec;
    }
}
