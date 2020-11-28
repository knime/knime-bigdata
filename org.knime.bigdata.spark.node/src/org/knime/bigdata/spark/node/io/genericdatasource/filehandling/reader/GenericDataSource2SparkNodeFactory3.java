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
 *   Created on Aug 10, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.filehandling.core.defaultnodesettings.filesystemchooser.FixedPortsConfiguration.FixedPortsConfigurationBuilder;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * Generic to Spark NIO node factory.
 *
 * @author Sascha Wolke, KNIME.com
 * @param <M> Model of this node factory
 * @param <S> Settings of this node factory
 */
public abstract class GenericDataSource2SparkNodeFactory3<M extends GenericDataSource2SparkNodeModel3<S>, S extends GenericDataSource2SparkSettings3>
    extends DefaultSparkNodeFactory<M> {

    /**
     * The name of the file system connection port group.
     */
    public static final String FS_INPUT_PORT_GRP_NAME = "File System Input Connection";

    /**
     * The name of the spark data port group.
     */
    static final String SPARK_INPUT_PORT_GRP_NAME = "Spark Input Data";

    /**
     * The name of the spark data port group.
     */
    static final String SPARK_OUTPUT_PORT_GRP_NAME = "Spark Output Data";

    /**
     * Fixed ports configuration.
     */
    protected static final PortsConfiguration PORTS_CONFIGURATION = new FixedPortsConfigurationBuilder() //
        .addFixedInputPortGroup(FS_INPUT_PORT_GRP_NAME, FileSystemPortObject.TYPE) //
        .addFixedInputPortGroup(SPARK_INPUT_PORT_GRP_NAME, SparkDataPortObject.TYPE) //
        .addFixedOutputPortGroup(SPARK_OUTPUT_PORT_GRP_NAME, SparkDataPortObject.TYPE) //
        .build();

    /**
     * Default Constructor.
     */
    public GenericDataSource2SparkNodeFactory3() {
        super("io/read");
    }

    /**
     * @return Initial settings object
     */
    public abstract S getSettings();

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<M> createNodeView(final int viewIndex, final M nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }
}
