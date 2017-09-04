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

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkDataView extends JPanel {

    private static final long serialVersionUID = 1L;

    /**
     * @param sparkData the {@link SparkDataTable} to visualize
     */
    SparkDataView(final SparkData sparkData) {
        super(new GridBagLayout());
        super.setName("Spark");
        final SparkContextID contextID = sparkData.getContextID();
        final StringBuilder buf = new StringBuilder("<html><body>");
        buf.append("<strong>Data</strong><hr>");
        buf.append("<strong>Named RDD:</strong>&nbsp;&nbsp;<tt>" + sparkData.getID() + "</tt><br/>");
        buf.append("<br/>");
        buf.append(SparkContextManager.getOrCreateSparkContext(contextID).getHTMLDescription());
        buf.append("</body></html>");
        final JTextPane textArea = new JTextPane();
        textArea.setContentType("text/html");
        textArea.setEditable(false);
        textArea.setText(buf.toString());
        textArea.setCaretPosition(0);
        final JScrollPane jsp = new JScrollPane(textArea);
        jsp.setPreferredSize(new Dimension(300, 300));
        final GridBagConstraints c = new GridBagConstraints();
        c.gridx = 0;
        c.gridy = 0;
        c.anchor = GridBagConstraints.CENTER;
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 1;
        c.weighty = 1;
        super.add(jsp, c);
    }

}
