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
package org.knime.bigdata.spark.core.port.model.ml;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;

import org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;
import org.knime.core.data.DataColumnSpec;

/**
 * Port view that shows the content of the Spark ML model port.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkMLModelSpecView extends JPanel {

    private static final long serialVersionUID = 1L;

    /**
     * @param spec the {@link SparkModelPortObjectSpec}
     */
    public SparkMLModelSpecView(final SparkMLModelPortObjectSpec spec) {
        super(new GridBagLayout());
        super.setName("ML Model");

        StringBuilder buf = new StringBuilder("<html><body>");
        appendField("Model name", spec.getModelName(), buf);
        appendField("Created with Spark version", spec.getSparkVersion().getLabel(), buf);
        if (spec.getTargetColumnSpec().isPresent()) {
            final DataColumnSpec targetCol = spec.getTargetColumnSpec().get();
            appendField("Target column",
                String.format("%s (%s)", targetCol.getName(), targetCol.getType().toPrettyString()), buf);
        }
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

    private static void appendField(final String fieldname, final String fieldValue, final StringBuilder buf) {
        buf.append(String.format("<p><strong>%s:</strong>&nbsp;&nbsp;%s</p>", fieldname, fieldValue));
    }
}
