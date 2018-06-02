package org.knime.bigdata.spark.core.livy.node.create;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

/**
 * Dialog panel to configure YARN container resources (memory and RAM). This dialog panel
 * has a companion settings class: {@link ContainerResourceSettings}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class ContainerResourceDialogPanel extends JPanel implements ChangeListener {

    private static final long serialVersionUID = -5194017291537188338L;

    private final CopyOnWriteArrayList<ChangeListener> m_listeners = new CopyOnWriteArrayList<>();
    
    private final DialogComponentBoolean m_overrideDefaultResources;
    
    private final DialogComponentNumber m_memory;
    
    private final DialogComponentStringSelection m_memoryUnit;
    
    private final DialogComponentNumber m_cores;
    
    /**
     * Constructor.
     * 
     * @param containerTypeText Text for the UI that describe what kind of container is being configured.
     * @param settings Settings instance.
     */
    public ContainerResourceDialogPanel(final String containerTypeText, final ContainerResourceSettings settings) {
        super(new GridBagLayout());
        
        m_overrideDefaultResources = new DialogComponentBoolean(
            settings.getOverrideDefaultModel(), String.format("Override default %s resources", containerTypeText));
        m_memory = new DialogComponentNumber(settings.getMemoryModel(), "", 1, 4);
        m_memoryUnit = new DialogComponentStringSelection(settings.getMemoryUnitModel(), "",
            Arrays.stream(MemoryUnit.values()).map(MemoryUnit::toString).toArray(String[]::new));
        m_cores = new DialogComponentNumber(settings.getCoresModel(), "", 1, 4);

        m_overrideDefaultResources.getModel().addChangeListener(this);
        m_memory.getModel().addChangeListener(this);
        m_memoryUnit.getModel().addChangeListener(this);
        m_cores.getModel().addChangeListener(this);
        
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0;
        gbc.weighty = 0;
        
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(5, 5, 0, 5);
        add(m_overrideDefaultResources.getComponentPanel(), gbc);
        
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 5, 0, 5);
        add(createResourcePanel(), gbc);
    }

    private JPanel createResourcePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0;
        gbc.weighty = 0;

        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(0, 20, 0, 0);
        panel.add(new JLabel("Memory:"), gbc);
        gbc.insets = new Insets(0, 0, 0, 0);

        gbc.gridx++;
        panel.add(m_memory.getComponentPanel(), gbc);

        gbc.gridx++;
        panel.add(m_memoryUnit.getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(0, 20, 0, 0);
        panel.add(new JLabel("Cores:"), gbc);
        gbc.insets = new Insets(0, 0, 0, 0);

        gbc.gridx++;
        panel.add(m_cores.getComponentPanel(), gbc);

        return panel;
    }

    /**
     * Adds a listener which is notified, whenever a new values is set in the model or the enable status changes. Does
     * nothing if the listener is already registered.
     *
     * @param l listener to add.
     */
    public void addChangeListener(final ChangeListener l) {
        if (!m_listeners.contains(l)) {
            m_listeners.add(l);
        }
    }
    
    @Override
    public void stateChanged(ChangeEvent e) {
        for (ChangeListener l : m_listeners) {
            l.stateChanged(new ChangeEvent(this));
        }
    }
}
