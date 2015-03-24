package net.tomp2p.holep.testapp;

import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JFrame;

public class HolePTestView extends JFrame {

	private static final long serialVersionUID = 1L;
	// Buttons
	private JButton getNatPeerAddressButton = new JButton("get peer2 PeerAddress");
	private JButton punchHoleButton = new JButton("send Message via Hole Punching");
	private JButton scriptStressTestButton = new JButton("StressTest");

	public HolePTestView(String frameName) {
		setLayout(new FlowLayout());
		
		add(getNatPeerAddressButton);
		add(punchHoleButton);
		add(scriptStressTestButton);
		
		setTitle(frameName);
		setSize(300, 400);
		setVisible(true);
	}

	public JButton getGetNatPeerAddressButton() {
		return getNatPeerAddressButton;
	}

	public JButton getPunchHoleButton() {
		return punchHoleButton;
	}
	
	public JButton getStressTestButton() {
		return scriptStressTestButton;
	}
}
