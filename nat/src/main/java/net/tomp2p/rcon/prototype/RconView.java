package net.tomp2p.rcon.prototype;

import java.awt.Button;
import java.awt.GridLayout;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class RconView {

	private JFrame frame = new JFrame();
	private JPanel pane = new JPanel();

	private Button sendMessageTestButton = new Button();
	private Button sendDirectedMessageButton = new Button();
	private Button sendDirectedNatPeerButton = new Button();
	private Button permanentPeerConnectionButton = new Button();
	private JTextField peerAddressField = new JTextField("my PeerAddress");
	private JTextField ipField = new JTextField("192.168.10.146");
	private JTextField idField = new JTextField("33");

	public JFrame make() {

		frame.setResizable(true);

		makePanel();

		frame.setVisible(true);

		return frame;
	}

	private void makePanel() {

		// sendMessageButton
		sendMessageTestButton.setEnabled(true);
		sendMessageTestButton.setLabel("Test message to masterpeer");

		peerAddressField.setToolTipText("My own PeerAddress");
		peerAddressField.setEditable(false);
		peerAddressField.setEnabled(false);
		peerAddressField.setText(SimpleRconClient.getPeer().peerAddress().toString());

		sendDirectedMessageButton.setEnabled(true);
		sendDirectedMessageButton.setLabel("Directed message");
		
		sendDirectedNatPeerButton.setEnabled(true);
		sendDirectedNatPeerButton.setLabel("Reverse connection setup");
		
		permanentPeerConnectionButton.setEnabled(true);
		permanentPeerConnectionButton.setLabel("Permanent PeerConnection");

		idField.setEditable(true);
		idField.setEnabled(true);

		ipField.setEditable(true);
		ipField.setEnabled(true);

		pane.setLayout(new GridLayout(7, 1));
		pane.add(peerAddressField, 0);
		pane.add(sendMessageTestButton, 1);
		pane.add(ipField, 2);
		pane.add(idField, 3);
		pane.add(sendDirectedMessageButton, 4);
		pane.add(sendDirectedNatPeerButton, 5);
		pane.add(permanentPeerConnectionButton, 6);

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

	public Button getSendDirectedNatPeerButton() {
		return sendDirectedNatPeerButton;
	}
	
	public Button getPermanentPeerConnectionButton() {
		return permanentPeerConnectionButton;
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
