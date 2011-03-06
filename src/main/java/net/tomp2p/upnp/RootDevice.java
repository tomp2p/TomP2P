/*
 * ====================================================================
 * ======== The Apache Software License, Version 1.1
 * ==================
 * ==========================================================
 * Copyright (C) 2002 The Apache Software Foundation. All rights
 * reserved. Redistribution and use in source and binary forms, with
 * or without modifica- tion, are permitted provided that the
 * following conditions are met: 1. Redistributions of source code
 * must retain the above copyright notice, this list of conditions and
 * the following disclaimer. 2. Redistributions in binary form must
 * reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other
 * materials provided with the distribution. 3. The end-user
 * documentation included with the redistribution, if any, must
 * include the following acknowledgment: "This product includes
 * software developed by SuperBonBon Industries
 * (http://www.sbbi.net/)." Alternately, this acknowledgment may
 * appear in the software itself, if and wherever such third-party
 * acknowledgments normally appear. 4. The names "UPNPLib" and
 * "SuperBonBon Industries" must not be used to endorse or promote
 * products derived from this software without prior written
 * permission. For written permission, please contact info@sbbi.net.
 * 5. Products derived from this software may not be called
 * "SuperBonBon Industries", nor may "SBBI" appear in their name,
 * without prior written permission of SuperBonBon Industries. THIS
 * SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR ITS
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLU- DING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE. This software consists of voluntary contributions made
 * by many individuals on behalf of SuperBonBon Industries. For more
 * information on SuperBonBon Industries, please see
 * <http://www.sbbi.net/>.
 */

package net.tomp2p.upnp;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;


import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Root UPNP device that is contained in a device definition file.
 * Slightly differs from a simple UPNPDevice object. This object will
 * contains all the child devices, this is the top objet in the UPNP
 * device devices hierarchy.
 * 
 * @author <a href="mailto:superbonbon@sbbi.net">SuperBonBon</a>
 * @version 1.0
 */

public class RootDevice extends Device
{

	/***/
	public final int specVersionMajor;

	/***/
	public final int specVersionMinor;

	private long validityTime;

	private long creationTime;

	/***/
	public final URL deviceDefLoc;

	private String deviceDefLocData;

	/***/
	public final String vendorFirmware;

	/***/
	public final String discoveryUSN;

	/***/
	public final String discoveryUDN;

	/**
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main( String[] args ) throws MalformedURLException
	{
		RootDevice root =
				build( new URL( "http://homepages.inf.ed.ac.uk/rmcnally/upnp.xml" ), "10", "vendor",
						"usn", "udn" );

		System.out.println( root );
	}

	/**
	 * @param deviceDef
	 * @param maxAge
	 * @param vendorFirmware
	 * @param discoveryUSN
	 * @param discoveryUDN
	 * @return a new {@link RootDevice}, or <code>null</code>
	 */
	public static RootDevice build( URL deviceDef, String maxAge, String vendorFirmware,
			String discoveryUSN, String discoveryUDN )
	{
		Document xml = XMLUtil.getXML( deviceDef );

		URL baseURL = null;

		try
		{
			String base = XMLUtil.xpath.evaluate( "/root/URLBase", xml );

			try
			{
				if( base != null && base.trim().length() > 0 )
				{
					baseURL = new URL( base );
				}
			}
			catch( MalformedURLException malformedEx )
			{
				// crappy urlbase provided
				// log.warn( "Error occured during device baseURL " + base
				// + " parsing, building it from device default location",
				// malformedEx );
				malformedEx.printStackTrace();
			}

			if( baseURL == null )
			{
				String URL =
						deviceDef.getProtocol() + "://" + deviceDef.getHost() + ":" + deviceDef.getPort();
				String path = deviceDef.getPath();
				if( path != null )
				{
					int lastSlash = path.lastIndexOf( '/' );
					if( lastSlash != -1 )
					{
						URL += path.substring( 0, lastSlash );
					}
				}
				try
				{
					baseURL = new URL( URL );
				}
				catch( MalformedURLException e )
				{
					e.printStackTrace();
				}
			}

			return new RootDevice( xml, baseURL, maxAge, deviceDef, vendorFirmware, discoveryUSN,
					discoveryUDN );
		}
		catch( XPathExpressionException e )
		{
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * @param doc
	 * @param urlBase
	 * @param maxAge
	 * @param deviceDefinition
	 * @param vendorFirmware
	 * @param discoveryUSN
	 * @param discoveryUDN
	 * @throws IllegalStateException
	 * @throws XPathExpressionException
	 */
	public RootDevice( Document doc, URL urlBase, String maxAge, URL deviceDefinition,
			String vendorFirmware, String discoveryUSN, String discoveryUDN )
			throws IllegalStateException, XPathExpressionException
	{
		super( ( Node ) XMLUtil.xpath.evaluate( "root/device", doc, XPathConstants.NODE ), null,
				urlBase );

		deviceDefLoc = deviceDefinition;

		validityTime = Integer.parseInt( maxAge ) * 1000;
		creationTime = System.currentTimeMillis();

		this.vendorFirmware = vendorFirmware;
		this.discoveryUSN = discoveryUSN;
		this.discoveryUDN = discoveryUDN;

		int svmaj = 0, svmin = 0;
		try
		{
			svmaj = Integer.parseInt( XMLUtil.xpath.evaluate( "root/specVersion/major", doc ) );
			svmin = Integer.parseInt( XMLUtil.xpath.evaluate( "root/specVersion/minor", doc ) );

			if( !( svmaj == 1 && svmin == 0 ) )
			{
				throw new IllegalStateException( "Unsupported device version (" + svmaj + "." + svmin
						+ ")" );
			}
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}

		specVersionMajor = svmaj;
		specVersionMinor = svmin;
	}

	/**
	 * The validity time for this device in milliseconds,
	 * 
	 * @return the number of milliseconds remaining before the device
	 *         object that has been build is considered to be outdated,
	 *         after this delay the UPNP device should resend an
	 *         advertisement message or a negative value if the device
	 *         is outdated
	 */
	public long getValidityTime()
	{
		long elapsed = System.currentTimeMillis() - creationTime;
		return validityTime - elapsed;
	}

	/**
	 * Resets the device validity time
	 * 
	 * @param newMaxAge
	 *           the maximum age in secs of this UPNP device before
	 *           considered to be outdated
	 */
	public void resetValidityTime( String newMaxAge )
	{
		validityTime = Integer.parseInt( newMaxAge ) * 1000;
		creationTime = System.currentTimeMillis();
	}

	/**
	 * Retrieves the device definition XML data
	 * 
	 * @return the device definition XML data as a String
	 */
	public String getDeviceDefinitionXML()
	{
		if( deviceDefLocData == null )
		{
			try
			{
				java.io.InputStream in = deviceDefLoc.openConnection().getInputStream();
				int readen = 0;
				byte[] buff = new byte[ 512 ];
				StringBuilder strBuff = new StringBuilder();
				while( ( readen = in.read( buff ) ) != -1 )
				{
					strBuff.append( new String( buff, 0, readen ) );
				}
				in.close();
				deviceDefLocData = strBuff.toString();
			}
			catch( IOException ioEx )
			{
				return null;
			}
		}
		return deviceDefLocData;
	}
}
