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

package net.sbbi.upnp.services;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import net.sbbi.upnp.XMLUtil;
import net.sbbi.upnp.messages.StateVariableMessage;
import net.sbbi.upnp.messages.UPNPMessageFactory;
import net.sbbi.upnp.messages.UPNPResponseException;

import org.w3c.dom.Node;

/**
 * Class to contain a service state variable definition
 * 
 * @author <a href="mailto:superbonbon@sbbi.net">SuperBonBon</a>
 * @version 1.0
 */

public class StateVariable implements StateVariableTypes
{

	private StateVariableMessage stateVarMsg = null;

	/**
	 * State variable name
	 */
	public final String name;

	/**
	 * Boolean to indicate if the variable is sending events when value
	 * of the var is changing.
	 */
	public final boolean sendEvents;

	/**
	 * The variable UPNP data type
	 */
	public final String dataType;

	/**
	 * The default value of the state variable
	 */
	public final String defaultValue;

	/**
	 * The minimum value as a string
	 */
	public final String minimumRangeValue;

	/**
	 * The maximum value as a string
	 */
	public final String maximumRangeValue;

	/**
	 * The value step range as a string
	 */
	public final String stepRangeValue;

	/**
	 * A set of allowed values (String objects) for the variable
	 */
	public final Set<String> allowedvalues = new HashSet<String>();

	/**
	 * The parent {@link Service} Object
	 */
	public final Service parent;

	StateVariable( Service parent, Node xml ) throws XPathExpressionException
	{
		this.parent = parent;
		sendEvents = Boolean.parseBoolean( XMLUtil.xpath.evaluate( "/@sendEvents", xml ) );

		name = XMLUtil.xpath.evaluate( "name", xml );
		dataType = XMLUtil.xpath.evaluate( "dataType", xml );
		defaultValue = XMLUtil.xpath.evaluate( "defaultValue", xml );
		minimumRangeValue = XMLUtil.xpath.evaluate( "allowedValueRange/minimum", xml );
		maximumRangeValue = XMLUtil.xpath.evaluate( "allowedValueRange/maximum", xml );
		stepRangeValue = XMLUtil.xpath.evaluate( "allowedValueRange/step", xml );

		Node allowedXML =
				( Node ) XMLUtil.xpath.evaluate( "allowedValueList", xml, XPathConstants.NODE );
		if( allowedXML != null )
		{
			int count =
					Integer.parseInt( XMLUtil.xpath.evaluate( "count( allowedValue )", allowedXML ) );
			for( int i = 1; i <= count; i++ )
			{
				String av = XMLUtil.xpath.evaluate( "allowedValue[ " + i + " ]", allowedXML );
				allowedvalues.add( av );
			}
		}
	}

	/**
	 * Call to the UPNP device to retrieve the state variable actual
	 * value
	 * 
	 * @return the state variable actual value on the device, should be
	 *         never null, an empty string could be returned by the
	 *         device
	 * @throws UPNPResponseException
	 *            if the device throws an exception during query
	 * @throws IOException
	 *            if some IO error with device occurs during query
	 */
	public String getValue() throws UPNPResponseException, IOException
	{
		if( stateVarMsg == null )
		{
			synchronized( this )
			{
				if( stateVarMsg == null )
				{
					UPNPMessageFactory factory = new UPNPMessageFactory( parent );
					stateVarMsg = factory.getStateVariableMessage( name );
				}
			}
		}
		return stateVarMsg.service().getStateVariableValue();
	}

	/**
	 * The variable JAVA data type (using an UPNP->Java mapping)
	 * 
	 * @return the class mapped
	 */
	public Class<?> getDataTypeAsClass()
	{
		return getDataTypeClassMapping( dataType );
	}

	static Class<?> getDataTypeClassMapping( String dataType )
	{
		int hash = dataType.hashCode();
		if( hash == UI1_INT )
		{
			return Short.class;
		}
		else if( hash == UI2_INT )
		{
			return Integer.class;
		}
		else if( hash == UI4_INT )
		{
			return Long.class;
		}
		else if( hash == I1_INT )
		{
			return Byte.class;
		}
		else if( hash == I2_INT )
		{
			return Short.class;
		}
		else if( hash == I4_INT )
		{
			return Integer.class;
		}
		else if( hash == INT_INT )
		{
			return Integer.class;
		}
		else if( hash == R4_INT )
		{
			return Float.class;
		}
		else if( hash == R8_INT )
		{
			return Double.class;
		}
		else if( hash == NUMBER_INT )
		{
			return Double.class;
		}
		else if( hash == FIXED_14_4_INT )
		{
			return Double.class;
		}
		else if( hash == FLOAT_INT )
		{
			return Float.class;
		}
		else if( hash == CHAR_INT )
		{
			return Character.class;
		}
		else if( hash == STRING_INT )
		{
			return String.class;
		}
		else if( hash == DATE_INT )
		{
			return Date.class;
		}
		else if( hash == DATETIME_INT )
		{
			return Date.class;
		}
		else if( hash == DATETIME_TZ_INT )
		{
			return Date.class;
		}
		else if( hash == TIME_INT )
		{
			return Date.class;
		}
		else if( hash == TIME_TZ_INT )
		{
			return Date.class;
		}
		else if( hash == BOOLEAN_INT )
		{
			return Boolean.class;
		}
		else if( hash == BIN_BASE64_INT )
		{
			return String.class;
		}
		else if( hash == BIN_HEX_INT )
		{
			return String.class;
		}
		else if( hash == URI_INT )
		{
			return URI.class;
		}
		else if( hash == UUID_INT )
		{
			return String.class;
		}
		return null;
	}

	static String getUPNPDataTypeMapping( String className )
	{
		if( className.equals( Short.class.getName() ) || className.equals( "short" ) )
		{
			return I2;
		}
		else if( className.equals( Byte.class.getName() ) || className.equals( "byte" ) )
		{
			return I1;
		}
		else if( className.equals( Integer.class.getName() ) || className.equals( "int" ) )
		{
			return INT;
		}
		else if( className.equals( Long.class.getName() ) || className.equals( "long" ) )
		{
			return UI4;
		}
		else if( className.equals( Float.class.getName() ) || className.equals( "float" ) )
		{
			return FLOAT;
		}
		else if( className.equals( Double.class.getName() ) || className.equals( "double" ) )
		{
			return NUMBER;
		}
		else if( className.equals( Character.class.getName() ) || className.equals( "char" ) )
		{
			return CHAR;
		}
		else if( className.equals( String.class.getName() ) || className.equals( "string" ) )
		{
			return STRING;
		}
		else if( className.equals( Date.class.getName() ) )
		{
			return DATETIME;
		}
		else if( className.equals( Boolean.class.getName() ) || className.equals( "boolean" ) )
		{
			return BOOLEAN;
		}
		else if( className.equals( URI.class.getName() ) )
		{
			return URI;
		}
		return null;
	}

	static Object UPNPToJavaObject( String dataType, String value ) throws Throwable
	{
		if( value == null )
		{
			throw new Exception( "null value" );
		}
		if( dataType == null )
		{
			throw new Exception( "null dataType" );
		}
		int hash = dataType.hashCode();
		if( hash == UI1_INT )
		{
			return new Short( value );
		}
		else if( hash == UI2_INT )
		{
			return new Integer( value );
		}
		else if( hash == UI4_INT )
		{
			return new Long( value );
		}
		else if( hash == I1_INT )
		{
			return new Byte( value );
		}
		else if( hash == I2_INT )
		{
			return new Short( value );
		}
		else if( hash == I4_INT )
		{
			return new Integer( value );
		}
		else if( hash == INT_INT )
		{
			return new Integer( value );
		}
		else if( hash == R4_INT )
		{
			return new Float( value );
		}
		else if( hash == R8_INT )
		{
			return new Double( value );
		}
		else if( hash == NUMBER_INT )
		{
			return new Double( value );
		}
		else if( hash == FIXED_14_4_INT )
		{
			return new Double( value );
		}
		else if( hash == FLOAT_INT )
		{
			return new Float( value );
		}
		else if( hash == CHAR_INT )
		{
			return new Character( value.charAt( 0 ) );
		}
		else if( hash == STRING_INT )
		{
			return value;
		}
		else if( hash == DATE_INT )
		{
			return ISO8601Date.parse( value );
		}
		else if( hash == DATETIME_INT )
		{
			return ISO8601Date.parse( value );
		}
		else if( hash == DATETIME_TZ_INT )
		{
			return ISO8601Date.parse( value );
		}
		else if( hash == TIME_INT )
		{
			return ISO8601Date.parse( value );
		}
		else if( hash == TIME_TZ_INT )
		{
			return ISO8601Date.parse( value );
		}
		else if( hash == BOOLEAN_INT )
		{
			if( value.equals( "1" ) || value.equalsIgnoreCase( "yes" ) || value.equals( "true" ) )
			{
				return Boolean.TRUE;
			}
			return Boolean.FALSE;
		}
		else if( hash == BIN_BASE64_INT )
		{
			return value;
		}
		else if( hash == BIN_HEX_INT )
		{
			return value;
		}
		else if( hash == URI_INT )
		{
			return new URI( value );
		}
		else if( hash == UUID_INT )
		{
			return value;
		}
		throw new Exception( "Unhandled data type " + dataType );
	}

	@Override
	public String toString()
	{
		return name + ":" + dataType;
	}

}
