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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;


import org.w3c.dom.Node;

/**
 * This class represents an UPNP device, this device contains a set of
 * services that will be needed to access the device functionalities.
 * 
 * @author <a href="mailto:superbonbon@sbbi.net">SuperBonBon</a>
 * @version 1.0
 */

public class Device
{
	/***/
	public final String deviceType;

	/***/
	public final String friendlyName;

	/***/
	public final String manufacturer;

	/***/
	public final URL manufacturerURL;

	/***/
	public final URL presentationURL;

	/***/
	public final String modelDescription;

	/***/
	public final String modelName;

	/***/
	public final String modelNumber;

	/***/
	public final String modelURL;

	/***/
	public final String serialNumber;

	/***/
	public final String UDN;

	/***/
	public final String USN;

	/***/
	public final long UPC;

	/***/
	public final Service[] services;

	/***/
	public final Device[] childDevices;

	/***/
	public final Device parent;

	/**
	 * @param deviceCtx
	 * @param parent
	 * @param urlBase
	 */
	public Device( Node deviceCtx, Device parent, URL urlBase )
	{
		deviceType = getMandatoryData( deviceCtx, "deviceType" );

		friendlyName = getMandatoryData( deviceCtx, "friendlyName" );
		manufacturer = getNonMandatoryData( deviceCtx, "manufacturer" );

		URL url = null;
		try
		{
			url = new URL( getNonMandatoryData( deviceCtx, "manufacturerURL" ) );
		}
		catch( java.net.MalformedURLException ex )
		{
			// crappy data provided, keep the field null
		}
		manufacturerURL = url;

		url = null;
		try
		{
			url = getURL( getNonMandatoryData( deviceCtx, "presentationURL" ), urlBase );
		}
		catch( java.net.MalformedURLException ex )
		{
			// crappy data provided, keep the field null
		}
		presentationURL = url;

		modelDescription = getNonMandatoryData( deviceCtx, "modelDescription" );
		modelName = getMandatoryData( deviceCtx, "modelName" );
		modelNumber = getNonMandatoryData( deviceCtx, "modelNumber" );
		modelURL = getNonMandatoryData( deviceCtx, "modelURL" );
		serialNumber = getNonMandatoryData( deviceCtx, "serialNumber" );
		UDN = getMandatoryData( deviceCtx, "UDN" );
		USN = UDN.concat( "::" ).concat( deviceType );

		String tmp = getNonMandatoryData( deviceCtx, "UPC" );
		long l = -1;
		if( tmp != null )
		{
			try
			{
				l = Long.parseLong( tmp );
			}
			catch( Exception ex )
			{
				// non all numeric field provided, non upnp compliant
				// device
			}
		}
		UPC = l;

		this.parent = parent;

		// service list
		List<Service> sList = new ArrayList<Service>();
		try
		{
			Node serviceList =
					( Node ) XMLUtil.xpath.evaluate( "serviceList", deviceCtx, XPathConstants.NODE );

			int serviceCount =
					Integer.parseInt( XMLUtil.xpath.evaluate( "count( service )", serviceList ) );

			for( int i = 1; i <= serviceCount; i++ )
			{
				Node serviceXML =
						( Node ) XMLUtil.xpath.evaluate( "service[" + i + "]", serviceList,
								XPathConstants.NODE );

				try
				{
					sList.add( new Service( serviceXML, urlBase, this ) );
				}
				catch( MalformedURLException e )
				{
					e.printStackTrace();
				}
			}
		}
		catch( XPathExpressionException e )
		{
			e.printStackTrace();
		}

		services = sList.toArray( new Service[ sList.size() ] );

		// child devices
		List<Device> children = new ArrayList<Device>();
		try
		{
			Node devList =
					( Node ) XMLUtil.xpath.evaluate( "deviceList", deviceCtx, XPathConstants.NODE );

			int devCount = Integer.parseInt( XMLUtil.xpath.evaluate( "count( device )", devList ) );

			for( int i = 1; i <= devCount; i++ )
			{
				Node devXML =
						( Node ) XMLUtil.xpath.evaluate( "device[" + i + "]", devList,
								XPathConstants.NODE );

				Device d = new Device( devXML, this, urlBase );
				children.add( d );
			}
		}
		catch( XPathExpressionException e )
		{
			// no sub-devices
		}

		childDevices = children.toArray( new Device[ children.size() ] );
	}

	private String getNonMandatoryData( Node ctx, String ctxFieldName )
	{
		String value = null;
		try
		{
			value = XMLUtil.xpath.evaluate( ctxFieldName, ctx );
		}
		catch( XPathExpressionException e )
		{
			e.printStackTrace();
		}

		if( value != null && value.length() == 0 )
		{
			value = null;
		}

		return value;
	}

	private static String getMandatoryData( Node ctx, String ctxFieldName )
	{
		String value = null;
		try
		{
			value = XMLUtil.xpath.evaluate( ctxFieldName, ctx );
		}
		catch( XPathExpressionException e )
		{
			e.printStackTrace();
		}

		if( value != null && value.length() == 0 )
		{
			throw new RuntimeException( "Mandatory field " + ctxFieldName
					+ " not provided, uncompliant UPNP device !!" );
		}

		return value;
	}

	/**
	 * Parsing an URL from the descriptionXML file
	 * 
	 * @param url
	 *           the string representation fo the URL
	 * @param baseURL
	 *           the base device URL, needed if the url param is
	 *           relative
	 * @return an URL object defining the url param
	 * @throws MalformedURLException
	 *            if the url param or baseURL.toExternalForm() + url
	 *            cannot be parsed to create an URL object
	 */
	public final static URL getURL( String url, URL baseURL ) throws MalformedURLException
	{
		URL rtrVal;
		if( url == null || url.trim().length() == 0 )
		{
			return null;
		}
		try
		{
			rtrVal = new URL( url );
		}
		catch( MalformedURLException malEx )
		{
			// maybe that the url is relative, we add the baseURL and
			// reparse it
			// if relative then we take the device baser url root and add
			// the url
			if( baseURL != null )
			{
				url = url.replace( '\\', '/' );
				if( url.charAt( 0 ) != '/' )
				{
					// the path is relative to the device baseURL
					String externalForm = baseURL.toExternalForm();
					if( !externalForm.endsWith( "/" ) )
					{
						externalForm += "/";
					}
					rtrVal = new URL( externalForm + url );
				}
				else
				{
					// the path is not relative
					String URLRoot =
							baseURL.getProtocol() + "://" + baseURL.getHost() + ":" + baseURL.getPort();
					rtrVal = new URL( URLRoot + url );
				}
			}
			else
			{
				throw malEx;
			}
		}
		return rtrVal;
	}

	/**
	 * Generates a list of all the child ( not only top level, full
	 * childrens hierarchy included ) UPNPDevice objects for this
	 * device.
	 * 
	 * @return the generated list or null if no child devices bound
	 */
	public List<Device> getChildDevices()
	{
		if( childDevices == null )
		{
			return null;
		}
		List<Device> rtrVal = new ArrayList<Device>();
		for( Device device : childDevices )
		{
			rtrVal.add( device );
			List<Device> found = device.getChildDevices();
			if( found != null )
			{
				rtrVal.addAll( found );
			}
		}
		return rtrVal;

	}

	/**
	 * Return the parent UPNPDevice, null if the device is an
	 * UPNPRootDevice
	 * 
	 * @return the parent device instance
	 */
	public Device getDirectParent()
	{
		return parent;
	}

	/**
	 * Looks for a child UPNP device definition file, the whole devices
	 * tree will be searched, starting from the current device node.
	 * 
	 * @param deviceURI
	 *           the device URI to search
	 * @return An UPNPDevice if anything matches or null
	 */
	public Device getChildDevice( String deviceURI )
	{
		if( deviceType.equals( deviceURI ) )
		{
			return this;
		}
		if( childDevices == null )
		{
			return null;
		}
		for( Device device : childDevices )
		{
			Device found = device.getChildDevice( deviceURI );
			if( found != null )
			{
				return found;
			}
		}
		return null;
	}

	/**
	 * Looks for a UPNP device service definition object for the given
	 * service URI (Type)
	 * 
	 * @param serviceURI
	 *           the URI of the service
	 * @return A matching UPNPService object or null
	 */
	public Service getService( String serviceURI )
	{
		if( services == null )
		{
			return null;
		}

		for( Service service : services )
		{
			if( service.serviceType.equals( serviceURI ) )
			{
				return service;
			}
		}
		return null;
	}

	/**
	 * Looks for a UPNP device service definition object for the given
	 * service ID
	 * 
	 * @param serviceID
	 *           the ID of the service
	 * @return A matching UPNPService object or null
	 */
	public Service getServiceByID( String serviceID )
	{
		if( services == null )
		{
			return null;
		}

		for( Service service : services )
		{
			if( service.serviceId.equals( serviceID ) )
			{
				return service;
			}
		}
		return null;
	}

	/**
	 * Looks for the all the UPNP device service definition object for
	 * the current UPNP device object. This method can be used to
	 * retrieve multiple same kind ( same service type ) of services
	 * with different services id on a device
	 * 
	 * @param serviceURI
	 *           the URI of the service
	 * @return A matching List of UPNPService objects or null
	 */
	public List<Service> getServices( String serviceURI )
	{
		if( services == null )
		{
			return null;
		}
		List<Service> rtrVal = new ArrayList<Service>();

		for( Service service : services )
		{
			if( service.serviceType.equals( serviceURI ) )
			{
				rtrVal.add( service );
			}
		}
		if( rtrVal.size() == 0 )
		{
			return null;
		}
		return rtrVal;
	}

	@Override
	public String toString()
	{
		StringBuilder b = new StringBuilder( "UPNP device " ).append( deviceType );
		b.append( " " ).append( friendlyName );
		b.append( "\n\t" ).append( manufacturer );
		b.append( " " ).append( manufacturerURL );
		b.append( " " ).append( presentationURL );
		b.append( "\n\t" ).append( modelName );
		b.append( " " ).append( modelNumber );
		b.append( " " ).append( modelDescription );
		b.append( " " ).append( modelURL );
		b.append( "\n\t" ).append( serialNumber );

		b.append( "\n\tServices:\n\t\t" );
		for( int i = 0; i < services.length; i++ )
		{
			String s = services[ i ].toString();
			s = s.replaceAll( "\n", "\n\t\t" );
			b.append( s );
			b.append( "\n\t\t" );
		}

		b.append( "\n\tDevices:\n\t\t" );
		for( int i = 0; i < childDevices.length; i++ )
		{
			String s = childDevices[ i ].toString();
			s = s.replaceAll( "\n", "\n\t" );
			b.append( s );
			b.append( "\n\t\t" );
		}

		return b.toString();
	}
}
