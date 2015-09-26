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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node;

import java.net.ConnectException;

import javax.ws.rs.ProcessingException;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.context.SparkContextPortObject;

/**
 * Abstract class for Spark source nodes. The node model adds an optional {@link SparkContextPortObject} to the
 * end of the input types array and takes care of the Spark context creation depending on the input ports.
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class SparkSourceNodeModel extends SparkNodeModel {

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected SparkSourceNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        this(inPortTypes, outPortTypes, true);
    }
    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     * @param deleteOnReset <code>true</code> if all output Spark RDDs should be deleted when the node is reseted
     * Always set this flag to <code>false</code> when you return the input RDD also as output RDD!
     */
    protected SparkSourceNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final boolean deleteOnReset) {
        super(inPortTypes, outPortTypes, deleteOnReset);
//        super(addContextPort(inPortTypes), outPortTypes, deleteOnReset);
    }

    /**
     * @param inTypes the input {@link PortType} array to add the context port to
     * @return the input {@link PortType} array plus the {@link SparkContextPortObject} type as last port
     */
    public static PortType[] addContextPort(final PortType[] inTypes) {
        final PortType[] types;
        if (inTypes == null) {
            types = new PortType[1];
        } else {
            types = new PortType[inTypes.length + 1];
            for (int i = 0, length = inTypes.length; i < length; i++) {
                types[i] = inTypes[i];
            }
        }
        types[types.length - 1] = SparkContextPortObject.TYPE_OPTIONAL;
        return types;
    }

    /**
     * @param in the {@link PortObjectSpec} array in configure or {@link PortObject} array in execute
     * @return the {@link KNIMESparkContext} to use
     * @throws InvalidSettingsException
     */
    public static KNIMESparkContext getContext(final Object[] in) throws InvalidSettingsException {
//        if (in != null && in.length >= 0 && (in[in.length - 1] instanceof SparkContextProvider)) {
//            return ((SparkContextProvider)in[in.length - 1]).getContext();
//        }
        try {
            return KnimeContext.getSparkContext();
        } catch (Exception e) {
            if (e instanceof ProcessingException) {
                final Throwable cause = e.getCause();
                if (cause != null && (cause instanceof ConnectException)) {
                    throw new InvalidSettingsException("Unable to connect to Spark job server. Exception: "
                            + cause.getMessage());
                }
            }
            if (e instanceof ConnectException) {
                throw new InvalidSettingsException("Unable to connect to Spark job server. Exception: "
                        + e.getMessage());
            }
            throw new InvalidSettingsException(e.getMessage());
        }
    }
}
