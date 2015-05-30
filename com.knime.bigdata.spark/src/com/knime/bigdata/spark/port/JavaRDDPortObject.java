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
 */
package com.knime.bigdata.spark.port;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.BufferedDataTableView;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.util.SwingWorkerWithContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;

/**
 * Class used as database port object holding a {@link BufferedDataTable} and a <code>ModelContentRO</code> to create a
 * database connection.
 *
 * @author Thomas Gabriel, University of Konstanz, modified by dwk
 */
public class JavaRDDPortObject extends DatabaseConnectionPortObject {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(JavaRDDPortObject.class);

    /**
     * Creates a new database port object.
     *
     * @param dbSpec database port object spec
     * @throws NullPointerException if one of the arguments is null
     */
    public JavaRDDPortObject(final DatabasePortObjectSpec dbSpec) {
        super(dbSpec);
    }

    /**
     * Database port type formed <code>PortObjectSpec.class</code> and <code>PortObject.class</code> from this class.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE = new PortType(JavaRDDPortObject.class);

    /** {@inheritDoc} */
    @Override
    public DatabasePortObjectSpec getSpec() {
        return (DatabasePortObjectSpec)m_spec;
    }

    /** {@inheritDoc} */
    @Override
    public String getSummary() {
        StringBuilder buf = new StringBuilder();
        buf.append("No. of columns: ").append(((DatabasePortObjectSpec)m_spec).getDataTableSpec().getNumColumns());
        final String dbId = m_spec.getDatabaseIdentifier();
        if (dbId != null) {
            buf.append(" DB: ").append(dbId);
        }
        return buf.toString();
    }

    /**
     * @return underlying data
     */
    private DataTable getDataTable(final int cacheNoRows) {
        try {
            String contextName = KnimeContext.getSparkContext();

            final String fetchParams = rowFetcherDef(cacheNoRows, getSpec().getDataTableSpec().getName());

            String jobId = JobControler.startJob(contextName, FetchRowsJob.class.getCanonicalName(), fetchParams);

            JobControler.waitForJob(jobId, null);

            assert (JobStatus.OK != JobControler.getJobStatus(jobId));

            return convertResultToDataTable(jobId);
        } catch (Throwable t) {
            LOGGER.error("Could not fetch data from Spark RDD, reason: " + t.getMessage(), t);
            return null;
        }
    }

    private String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_TABLE_1,
                aTableName}});
    }

    private DataTable convertResultToDataTable(final String aJobId) throws GenericKnimeSparkException {

        JobResult statusWithResult = JobControler.fetchJobResult(aJobId);
        assert (statusWithResult != null); //row fetcher must return something
        assert ("OK".equals(statusWithResult.getMessage())); //fetcher should return OK as result status
        final Object[][] arrayRes = (Object[][])statusWithResult.getObjectResult();
        assert (arrayRes != null) : "Row fetcher failed to return a result";

        return new DataTable() {

            @Override
            public RowIterator iterator() {
                return new RowIterator() {

                    private int currentRow = 0;

                    @Override
                    public DataRow next() {
                        final Object[] o = arrayRes[currentRow];
                        currentRow++;
                        return new DataRow() {

                            @Override
                            public Iterator<DataCell> iterator() {
                                return new Iterator<DataCell>() {
                                    private int current = 0;

                                    @Override
                                    public boolean hasNext() {
                                        return current < o.length;
                                    }

                                    @Override
                                    public DataCell next() {
                                        DataCell cell = getCell(current);
                                        current++;
                                        return cell;
                                    }

                                    @Override
                                    public void remove() {
                                        throw new UnsupportedOperationException();
                                    }
                                };
                            }

                            @Override
                            public int getNumCells() {
                                return o.length;
                            }

                            @Override
                            public RowKey getKey() {
                                // TODO Auto-generated method stub
                                return null;
                            }

                            @Override
                            public DataCell getCell(final int index) {
                                return new MyRDDDataCell(o, index);
                            }
                        };
                    }

                    @Override
                    public boolean hasNext() {
                        return currentRow < arrayRes.length;
                    }
                };
            }

            @Override
            public DataTableSpec getDataTableSpec() {
                final Object[] o = arrayRes[0];
                final String[] names = new String[o.length];
                final DataType[] types = new DataType[o.length];
                for (int i=0; i<o.length; i++) {
                    names[i] = "RDD-col"+i;
                    types[i] = DataType.getType(MyRDDDataCell.class);
                }
                return new DataTableSpec(names, types);
            }
        };
    }

    private static class MyRDDDataCell extends DataCell {
        private final int m_index;
        private final Object[] m_row;

        MyRDDDataCell(final Object[] aRow, final int aIndex) {
            m_index = aIndex;
            m_row = aRow;
        }
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public String toString() {
            return m_row[m_index].toString();
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        protected boolean equalsDataCell(final DataCell dc) {
            return (dc != null && dc.toString().equals(toString()));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.10
     */
    @Override
    public DatabaseQueryConnectionSettings getConnectionSettings(final CredentialsProvider credProvider)
        throws InvalidSettingsException {
        return ((DatabasePortObjectSpec)m_spec).getConnectionSettings(credProvider);
    }

    /**
     * Serializer used to save <code>JavaRDDPortObject</code>.
     *
     * @return a new database port object serializer
     */
    public static PortObjectSerializer<DatabaseConnectionPortObject> getPortObjectSerializer() {
        return new PortObjectSerializer<DatabaseConnectionPortObject>() {
            /** {@inheritDoc} */
            @Override
            public void savePortObject(final DatabaseConnectionPortObject portObject,
                final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
                CanceledExecutionException {
                // nothing to save
            }

            /** {@inheritDoc} */
            @Override
            public JavaRDDPortObject loadPortObject(final PortObjectZipInputStream in, final PortObjectSpec spec,
                final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
                return new JavaRDDPortObject((DatabasePortObjectSpec)spec);
            }
        };
    }

    /**
     * Override this panel in order to set the CredentialsProvider into this class.
     */
    @SuppressWarnings("serial")
    public final class DatabaseOutPortPanel extends JPanel {
        /**
         * Create new database provider.
         *
         * @param lm using this layout manager
         */
        public DatabaseOutPortPanel(final LayoutManager lm) {
            super(lm);
        }

    }

    /** {@inheritDoc} */
    @Override
    public JComponent[] getViews() {
        JComponent[] superViews = super.getViews();

        final JComponent[] panels = new JComponent[superViews.length + 1];
        @SuppressWarnings("serial")
        final BufferedDataTableView dataView = new BufferedDataTableView(null) {
            @Override
            public String getName() {
                return "Table Preview";
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
                panels[0].add(new JLabel("Fetching " + value.get() + " rows from database..."), BorderLayout.NORTH);
                panels[0].repaint();
                panels[0].revalidate();
                new SwingWorkerWithContext<DataTable, Void>() {
                    /** {@inheritDoc} */
                    @Override
                    protected DataTable doInBackgroundWithContext() throws Exception {
                        return getDataTable(value.get());
                    }

                    /** {@inheritDoc} */
                    @Override
                    protected void doneWithContext() {
                        DataTable dt = null;
                        try {
                            dt = super.get();
                        } catch (ExecutionException ee) {
                            LOGGER.warn("Error during fetching data from " + "database, reason: " + ee.getMessage(), ee);
                        } catch (InterruptedException ie) {
                            LOGGER.warn("Error during fetching data from " + "database, reason: " + ie.getMessage(), ie);
                        }
                        @SuppressWarnings("serial")
                        final BufferedDataTableView dataView2 = new BufferedDataTableView(dt) {
                            /** {@inheritDoc} */
                            @Override
                            public String getName() {
                                return "Table Preview";
                            }
                        };
                        dataView2.setName("Table Preview");
                        panels[0].removeAll();
                        panels[0].add(p, BorderLayout.NORTH);
                        panels[0].add(dataView2, BorderLayout.CENTER);
                        panels[0].setName(dataView2.getName());
                        panels[0].repaint();
                        panels[0].revalidate();
                    }
                }.execute();
            }
        });
        for (int i = 1; i < panels.length; i++) {
            panels[i] = superViews[i - 1];
        }
        return panels;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof JavaRDDPortObject)) {
            return false;
        }
        return super.equals(obj);
    }

}
