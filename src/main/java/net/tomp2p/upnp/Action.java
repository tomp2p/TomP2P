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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import net.tomp2p.upnp.Argument.Direction;

import org.w3c.dom.Node;

/**
 * An object to represent a service action proposed by an UPNP service
 * 
 * @author <a href="mailto:superbonbon@sbbi.net">SuperBonBon</a>
 * @version 1.0
 */
public class Action
{
	/***/
	public final String name;

	/***/
	public final Service parent;

	/***/
	public final Argument[] arguments;

	private Argument[] input, output;

	Action( Service parent, Node xml ) throws XPathExpressionException
	{
		this.parent = parent;
		XPath xpath = XMLUtil.xpath;

		name = xpath.evaluate( "name", xml );

		Node argList = ( Node ) xpath.evaluate( "argumentList", xml, XPathConstants.NODE );

		int argCount =
				argList == null ? 0 : Integer.parseInt( xpath.evaluate( "count( argument )", argList ) );
		arguments = new Argument[ argCount ];

		for( int i = 1; i <= argCount; i++ )
		{
			Node argXML =
					( Node ) xpath.evaluate( "argument[ " + i + " ]", argList, XPathConstants.NODE );
			arguments[ i - 1 ] = new Argument( argXML );
		}
	}

	/**
	 * Look for an {@link Argument} for a given name
	 * 
	 * @param argumentName
	 *           the argument name
	 * @return the argument or null if not found or not available
	 */
	public Argument getActionArgument( String argumentName )
	{
		for( Argument arg : arguments )
		{
			if( arg.name.equals( argumentName ) )
			{
				return arg;
			}
		}
		return null;
	}

	/**
	 * Return a list containing input ( when a response is sent )
	 * arguments objects
	 * 
	 * @return a list containing input arguments ServiceActionArgument
	 *         objects or null when nothing is needed for such
	 *         operation
	 */
	public Argument[] getInputActionArguments()
	{
		if( input == null )
		{
			List<Argument> l = new ArrayList<Argument>();

			for( Argument a : arguments )
			{
				if( a.direction == Direction.in )
				{
					l.add( a );
				}
			}
			input = l.toArray( new Argument[ l.size() ] );
		}

		return input;
	}

	/**
	 * Look for an input ServiceActionArgument for a given name
	 * 
	 * @param argumentName
	 *           the input argument name
	 * @return the argument or null if not found or not available
	 */
	public Argument getInputActionArgument( String argumentName )
	{
		Argument[] saa = getInputActionArguments();

		for( int i = 0; i < saa.length; i++ )
		{
			if( saa[ i ].name.equals( argumentName ) )
			{
				return saa[ i ];
			}
		}

		return null;
	}

	/**
	 * Return a list containing output ( when a response is received )
	 * arguments objects
	 * 
	 * @return a list containing output arguments ServiceActionArgument
	 *         objects or null when nothing returned for such operation
	 */
	public Argument[] getOutputActionArguments()
	{
		if( output == null )
		{
			List<Argument> l = new ArrayList<Argument>();

			for( Argument a : arguments )
			{
				if( a.direction == Direction.out )
				{
					l.add( a );
				}
			}
			output = l.toArray( new Argument[ l.size() ] );
		}

		return output;
	}

	/**
	 * Look for an output {@link Argument} for a given name
	 * 
	 * @param argumentName
	 *           the input argument name
	 * @return the {@link Argument} or null if not found or not
	 *         available
	 */
	public Argument getOutputActionArgument( String argumentName )
	{
		Argument[] saa = getOutputActionArguments();

		for( int i = 0; i < saa.length; i++ )
		{
			if( saa[ i ].name.equals( argumentName ) )
			{
				return saa[ i ];
			}
		}

		return null;
	}

	/**
	 * The action name
	 * 
	 * @return The action name
	 */
	public String getName()
	{
		return name;
	}

	@Override
	public String toString()
	{
		return name + Arrays.toString( arguments );
	}
}
