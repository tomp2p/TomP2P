package net.tomp2p.holep.testapp;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

public class HolePTestController {

	private final HolePTestView view;
	private final HolePTestApp app;
	
	public HolePTestController(String framename, final HolePTestApp app) {
		this.view = new HolePTestView(framename);
		this.app = app;
		
		this.view.getGetNatPeerAddressButton().addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				System.err.println("Button \"getNatPeerAddress pressed!\"");
				try {
					app.getOtherPeerAddress();
				} catch (ClassNotFoundException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		});
		
		this.view.getPunchHoleButton().addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				System.err.println("Button \"PunchHole pressed!\"");
				try {
					app.sendHolePMessage();
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		});
	}
	
	

}
