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
package com.knime.bigdata.spark.port.data;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingWorker;

import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.BufferedDataTableView;
import org.knime.core.node.workflow.DataTableSpecView;

import com.knime.bigdata.spark.port.SparkContextProvider;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.util.SparkDataTableCreator;

/**
 * Spark data {@link PortObject} implementation which holds a reference to a {@link SparkDataTable} object.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkDataPortObject implements PortObject, SparkContextProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkDataPortObject.class);

    /**
     * Database port type.
     */
    public static final PortType TYPE = new PortType(SparkDataPortObject.class);

    /**
     * Database type for optional ports.
     */
    public static final PortType TYPE_OPTIONAL = new PortType(SparkDataPortObject.class, true);

    /**
     * Serializer used to save {@link SparkDataPortObject}s.
     *
     * @return a new serializer
     */
    public static PortObjectSerializer<SparkDataPortObject> getPortObjectSerializer() {
        return new PortObjectSerializer<SparkDataPortObject>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void savePortObject(final SparkDataPortObject portObject,
                final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
                CanceledExecutionException {
                portObject.m_data.save(out);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public SparkDataPortObject loadPortObject(final PortObjectZipInputStream in,
                final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
                return new SparkDataPortObject(new SparkDataTable(in));
            }
        };
    }
    private final SparkDataTable m_data;

    /**
     * @param data
     */
    public SparkDataPortObject(final SparkDataTable data) {
        m_data = data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkDataPortObjectSpec getSpec() {
        return new SparkDataPortObjectSpec(m_data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        StringBuilder buf = new StringBuilder();
        buf.append("Cols " + getTableSpec().getNumColumns()
            + "Context " + getContext().getContextName() + " ID " + getData().getID());
        return buf.toString();
    }

    /**
     * @return the model
     */
    public SparkDataTable getData() {
        return m_data;
    }

    /**
     * Override this panel in order to set the CredentialsProvider
     * into this class.
     */
    @SuppressWarnings("serial")
    public final class DatabaseOutPortPanel extends JPanel {
        /**
         * Create new database provider.
         * @param lm using this layout manager
         */
        public DatabaseOutPortPanel(final LayoutManager lm) {
            super(lm);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        final JComponent[] superViews =
                new JComponent[]{new DataTableSpecView(getTableSpec()), new SparkDataView(m_data)};
        final JComponent[] panels = new JComponent[superViews.length + 1];
        @SuppressWarnings("serial")
        final BufferedDataTableView dataView = new BufferedDataTableView(null) {
            @Override
            public String getName() {
                return "Data Preview";
            }
        };
        final JButton b = new JButton("Cache no. of rows: ");
        final JPanel p = new JPanel(new FlowLayout());
        final JTextField cacheRows = new JTextField("100");
        cacheRows.setMinimumSize(new Dimension(50, 20));
        cacheRows.setPreferredSize(new Dimension(50, 20));
        p.add(b);
        p.add(cacheRows);
        panels[0] = new DatabaseOutPortPanel(new BorderLayout());
        panels[0].setName(dataView.getName());
        panels[0].add(p, BorderLayout.NORTH);
        panels[0].add(dataView, BorderLayout.CENTER);
        b.addActionListener(new ActionListener() {
            /** {@inheritDoc} */
            @Override
            public void actionPerformed(final ActionEvent e) {
                final AtomicInteger value = new AtomicInteger(100);
                try {
                    int v = Integer.parseInt(cacheRows.getText().trim());
                    value.set(v);
                } catch (NumberFormatException nfe) {
                    cacheRows.setText(Integer.toString(value.get()));
                }
                panels[0].removeAll();
                panels[0].add(new JLabel("Fetching " + value.get() + " rows from Spark..."), BorderLayout.NORTH);
                panels[0].repaint();
                panels[0].revalidate();
                //TK_TODO: Add job cancel button to the dialog to allow users to stop the fetching job
                final SwingWorker<DataTable, Void> worker  = new SwingWorker<DataTable, Void>() {
                    /** {@inheritDoc} */
                    @Override
                    protected DataTable doInBackground() throws Exception {
                        return SparkDataTableCreator.getDataTable(null, getData(), value.get());
                    }
                    /** {@inheritDoc} */
                    @Override
                    protected void done() {
                        DataTable dt = null;
                        try {
                            dt = super.get();
                        } catch (ExecutionException|InterruptedException ee) {
                            LOGGER.warn("Error during fetching data from Spark, reason: " + ee.getMessage(), ee);
                            final Throwable cause = ee.getCause();
                            final String msg;
                            if (cause != null) {
                                msg = cause.getMessage();
                            } else {
                                msg = ee.getMessage();
                            }
                            panels[0].removeAll();
                            panels[0].add(new JLabel("Error fetching rows from Spark: " + msg), BorderLayout.NORTH);
                            panels[0].repaint();
                            panels[0].revalidate();
                            return;
                        }
                        if (dt == null) {
                            panels[0].removeAll();
                            panels[0].add(new JLabel("Error fetching " + value.get()
                                    + " rows from Spark. For details see log file."), BorderLayout.NORTH);
                            panels[0].repaint();
                            panels[0].revalidate();
                        } else {
                            @SuppressWarnings("serial")
                            final BufferedDataTableView dataView2 = new BufferedDataTableView(dt) {
                                /** {@inheritDoc} */
                                @Override
                                public String getName() {
                                    return "Data Preview";
                                }
                            };
                            dataView2.setName("Data Preview");
                            panels[0].removeAll();
                            panels[0].add(p, BorderLayout.NORTH);
                            panels[0].add(dataView2, BorderLayout.CENTER);
                            panels[0].setName(dataView2.getName());
                            panels[0].repaint();
                            panels[0].revalidate();
                        }
                    }
                };
                worker.execute();
            }
        });
        for (int i = 1; i < panels.length; i++) {
            panels[i] = superViews[i - 1];
        }
        return panels;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SparkDataPortObject)) {
            return false;
        }
        SparkDataPortObject port = (SparkDataPortObject) obj;
        return m_data.equals(port.m_data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return m_data.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KNIMESparkContext getContext() {
        return m_data.getContext();
    }

    /**
     * @return the unique table name
     */
    public String getTableName() {
        return m_data.getID();
    }

    /**
     * @return the {@link DataTableSpec} of the result table
     */
    public DataTableSpec getTableSpec(){
        return m_data.getTableSpec();
    }
}
