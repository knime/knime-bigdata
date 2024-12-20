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
 *   Created on 13.09.2015 by koetter
 */
package org.knime.bigdata.spark.core.node;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.version.SparkProvider;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Abstract class for Spark source nodes. The node model adds an optional {@link SparkContextPortObject} to the
 * end of the input types array and takes care of the Spark context creation depending on the input ports.
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class SparkSourceNodeModel extends SparkNodeModel {

    /**
     * Constructor for Spark source node model base class.
     *
     * @param inPortTypes the input port types
     * @param optionalSparkPort true if input spark context port is optional
     * @param outPortTypes the output port types
     */
    protected SparkSourceNodeModel(final PortType[] inPortTypes, final boolean optionalSparkPort, final PortType[] outPortTypes) {
        this(inPortTypes, optionalSparkPort, outPortTypes, true);
    }

    /**
     * Constructor for Spark source node model base class.
     *
     * @param inPortTypes the input port types
     * @param optionalSparkPort true if input spark context port is optional
     * @param outPortTypes the output port types
     * @param deleteOnReset <code>true</code> if all output Spark RDDs should be deleted when the node is reseted
     * Always set this flag to <code>false</code> when you return the input RDD also as output RDD!
     */
    protected SparkSourceNodeModel(final PortType[] inPortTypes, final boolean optionalSparkPort, final PortType[] outPortTypes,
        final boolean deleteOnReset) {
        super(addContextPort(inPortTypes, optionalSparkPort), outPortTypes, deleteOnReset);
    }

    /**
     * @param inTypes the input {@link PortType} array to add the context port to
     * @param optionalSparkPort true if input spark context port is optional
     * @return the input {@link PortType} array plus the {@link SparkContextPortObject} type as last port
     */
    public static PortType[] addContextPort(final PortType[] inTypes, final boolean optionalSparkPort) {
       final PortType[] types;
        if (inTypes == null) {
            types = new PortType[1];
        } else {
            types = new PortType[inTypes.length + 1];
            for (int i = 0, length = inTypes.length; i < length; i++) {
                types[i] = inTypes[i];
            }
        }

        if (optionalSparkPort) {
            types[types.length - 1] = SparkContextPortObject.TYPE_OPTIONAL;
        } else {
            types[types.length - 1] = SparkContextPortObject.TYPE;
        }

        return types;
    }


    /**
     *
     * @param in an array of {@link PortObjectSpec} or {@link PortObject} instances
     * @return A {@link SparkContextID} taken from the first inObject that is a {@link SparkProvider}.
     * @throws InvalidSettingsException if no input connection found that is a {@link SparkProvider}
     */
    public static SparkContextID getContextID(final Object[] in) throws InvalidSettingsException {
        if (in != null && in.length > 0 && (in[in.length - 1] instanceof SparkContextProvider)) {
            return ((SparkContextProvider)in[in.length - 1]).getContextID();
        }

        throw new InvalidSettingsException("Spark input connection required");
    }

    /**
     *
     * @param contextID the contextID to ensure.
     * @param exec ExecutionMonitor to track progress.
     * @throws KNIMESparkException
     * @throws CanceledExecutionException
     */
    public static void ensureContextIsOpen(final SparkContextID contextID, final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        SparkContextManager.getOrCreateSparkContext(contextID).ensureOpened(true, exec);
    }

}
