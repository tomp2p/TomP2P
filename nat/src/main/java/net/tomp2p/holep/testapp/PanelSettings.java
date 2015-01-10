package net.tomp2p.holep.testapp;

import java.awt.*;

import javax.swing.*;
import javax.swing.border.EmptyBorder;

public class PanelSettings {

	public static void build(JPanel view, LayoutManager layout, double width,
			double height, Color background, int border, boolean opaque) {

		if (layout != null)
			view.setLayout(layout);

		if (width >= 0 && height >= 0) {
			view.setPreferredSize(new Dimension((int) width, (int) height));
			view.setMinimumSize(new Dimension((int) width, (int) height));
			view.setMaximumSize(new Dimension((int) width, (int) height));
		}

		if (background != null)
			view.setBackground(background);

		if (border >= 0)
			view.setBorder(new EmptyBorder(border, border, border, border));

		view.setOpaque(opaque);

	}

}
