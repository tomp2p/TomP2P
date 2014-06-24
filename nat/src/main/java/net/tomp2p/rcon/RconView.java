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

	private Button sendMessageTestButton = new Button();
	private Button sendDirectedMessageButton = new Button();
	private JTextField peerAddressField = new JTextField("my PeerAddress");
	private JTextField ipField = new JTextField("Enter IP of recepient");
	private JTextField idField = new JTextField("Enter ID of recepient");

	public JFrame make() {

		frame.setResizable(true);

		makePanel();

		frame.setVisible(true);

		return frame;
	}

	private void makePanel() {

		//sendMessageButton
		sendMessageTestButton.setEnabled(true);
		sendMessageTestButton.setLabel("sendTestMessage()");
		
		peerAddressField.setToolTipText("My own PeerAddress");
		peerAddressField.setEditable(false);
		peerAddressField.setEnabled(false);
		peerAddressField.setText(SimpleRconClient.getPeer().peerAddress().toString());

		sendDirectedMessageButton.setEnabled(true);
		sendDirectedMessageButton.setLabel("sendDirectedMessage()");
		
		idField.setEditable(true);
		idField.setEnabled(true);

		ipField.setEditable(true);
		ipField.setEnabled(true);
		
		pane.setLayout(new GridLayout(5, 1));
		pane.add(peerAddressField, 0);
		pane.add(sendMessageTestButton, 1);
		pane.add(ipField, 2);
		pane.add(idField, 3);
		pane.add(sendDirectedMessageButton, 4);

		frame.add(pane);
	}

	public JFrame getJFrame() {
		return frame;
	}
	
	public Button getSendTestMessageButton() {
		return sendMessageTestButton;
	}

	public Button getSendDirectedMessageButton() {
		return sendDirectedMessageButton;
	}

	public JTextField getPeerAddressField() {
		return peerAddressField;
	}

	public JTextField getIpField() {
		return ipField;
	}

	public JTextField getIdField() {
		return idField;
	}
	
	
}
