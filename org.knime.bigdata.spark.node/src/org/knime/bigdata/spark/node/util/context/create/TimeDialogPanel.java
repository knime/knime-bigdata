package org.knime.bigdata.spark.node.util.context.create;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.node.util.context.create.TimeSettings.TimeShiftStrategy;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.time.util.DialogComponentTimeZoneSelection;

/**
 * Dialog panel to configure time shift between cluster and client.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class TimeDialogPanel extends JPanel {

    private static final long serialVersionUID = -5194017291537188338L;

    private final DialogComponentButtonGroup m_timeShiftStrategy;

    private final DialogComponentTimeZoneSelection m_fixedTimeZone;

    private final DialogComponentBoolean m_failOnDiffFixedTimeZone;

    private final DialogComponentBoolean m_failOnDiffClientTimeZone;

    /**
     * Default constructor.
     *
     * @param settings Settings instance.
     */
    public TimeDialogPanel(final TimeSettings settings) {
        super(new GridBagLayout());

        m_timeShiftStrategy = new DialogComponentButtonGroup(settings.getTimeShiftStrategyModel(), null, false,
            getTimeShiftStrategyText(), getTimeShiftStrategyActionCommand());
        m_timeShiftStrategy.getModel().addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                settings.updateEnabledness();
            }
        });
        m_fixedTimeZone = new DialogComponentTimeZoneSelection(settings.getFixedTimeZoneModel(), null, null);
        m_failOnDiffFixedTimeZone = new DialogComponentBoolean(settings.getFixedTZFailOnDifferenClusterTZModel(),
                "fail on different cluster default time zone");
        m_failOnDiffClientTimeZone = new DialogComponentBoolean(settings.getClientTZFailOnDifferenClusterTZModel(),
                "fail on different cluster default time zone");

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;

        add(createTimeShiftPanel(), gbc);
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        add(new JLabel(""), gbc); // add some space
    }

    private JPanel createTimeShiftPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Time shift between KNIME and Spark"));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 0;
        gbc.weighty = 0;

        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(5, 20, 5, 20);
        panel.add(m_timeShiftStrategy.getButton(TimeShiftStrategy.NONE.getActionCommand()), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_timeShiftStrategy.getButton(TimeShiftStrategy.DEFAULT_CLUSTER.getActionCommand()), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 20, 0, 20);
        final JPanel box = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        box.add(m_timeShiftStrategy.getButton(TimeShiftStrategy.FIXED.getActionCommand()));
        box.add(m_fixedTimeZone.getComponentPanel());
        panel.add(box, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(0, 20, 0, 20);
        panel.add(m_failOnDiffFixedTimeZone.getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 20, 0, 20);
        panel.add(m_timeShiftStrategy.getButton(TimeShiftStrategy.DEFAULT_CLIENT.getActionCommand()), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(0, 20, 0, 20);
        panel.add(m_failOnDiffClientTimeZone.getComponentPanel(), gbc);

        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        JPanel space = new JPanel();
        panel.add(space, gbc);

        return panel;
    }

    private static final String[] getTimeShiftStrategyText() {
        final TimeShiftStrategy[] values = TimeShiftStrategy.values();
        final String[] text = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            text[i] = values[i].getText();
        }
        return text;
    }

    private static final String[] getTimeShiftStrategyActionCommand() {
        final TimeShiftStrategy[] values = TimeShiftStrategy.values();
        final String[] actionCommands = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            actionCommands[i] = values[i].getActionCommand();
        }
        return actionCommands;
    }
}
