package net.tomp2p.rcon;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.JFrame;

import net.tomp2p.rcon.prototype.SimpleRconClient;

public class RconController {

	private RconView rconView;
	
	public void start() {
		rconView = new RconView();
		rconView.make();
		rconView.getJFrame().setSize(800, 600);
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				System.out.println("useless, because Hook runs in a separate Thread...");
			}
		}, "Shutdown-thread"));
		
		rconView.getSendMessageButton().addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				System.out.println("Button pressed");
				try {
					SimpleRconClient.sendDummy("this is a Dummy");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}
}
