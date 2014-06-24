package net.tomp2p.rcon;

import java.awt.Button;
import java.awt.GridLayout;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;

import net.tomp2p.rcon.prototype.SimpleRconClient;

public class RconView {

	private JFrame frame = new JFrame();
	private JPanel pane = new JPanel();

	private Button sendMessageButton = new Button();
	private JTextField peerAddressField = new JTextField("my PeerAddress");

	public JFrame make() {

		frame.setResizable(true);
		frame.setLayout(new GridLayout(1, 1));

		makePanel();

		frame.setVisible(true);

		return frame;
	}

	private void makePanel() {

		sendMessageButton.setEnabled(true);
		sendMessageButton.setLabel("sendMessage()");
		
		peerAddressField.setEditable(false);
		peerAddressField.setEnabled(false);
		peerAddressField.setText(SimpleRconClient.getPeer().peerAddress().toString());

		pane.add(peerAddressField);
		pane.add(sendMessageButton);

		frame.add(pane);
	}

	public JFrame getJFrame() {
		return frame;
	}
	
	public Button getSendMessageButton() {
		return sendMessageButton;
	}

}
