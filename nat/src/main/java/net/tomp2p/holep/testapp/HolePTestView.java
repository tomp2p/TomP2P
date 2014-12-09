package net.tomp2p.holep.testapp;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Label;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

public class HolePTestView extends JFrame {

	// Buttons
	private JButton getNatPeerAddressButton = new JButton("get peer2 PeerAddress");
	private JButton punchHoleButton = new JButton("punch a hole on port xy");

	// Textfields
	private static JTextArea punchHolePort = new JTextArea("8080");
	private static Label punchHolePort_text = new Label("enter a valid Port");

	public HolePTestView(String frameName) {
		setLayout(new FlowLayout());
		
		add(getNatPeerAddressButton);
		add(punchHolePort_text);
		add(punchHolePort);
		add(punchHoleButton);
		
		setTitle(frameName);
//		setPreferredSize(new Dimension(500, 500));
		setSize(300, 400);
		setVisible(true);
	}

	public JButton getGetNatPeerAddressButton() {
		return getNatPeerAddressButton;
	}

	public JButton getPunchHoleButton() {
		return punchHoleButton;
	}

	public static JTextArea getPunchHolePort() {
		return punchHolePort;
	}

	public static Label getPunchHolePort_text() {
		return punchHolePort_text;
	}
}
