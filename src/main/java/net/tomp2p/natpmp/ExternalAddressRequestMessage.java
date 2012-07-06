/*
 * This file is part of jNAT-PMPlib.
 *
 * jNAT-PMPlib is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * jNAT-PMPlib is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with jNAT-PMPlib.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.tomp2p.natpmp;

import java.net.Inet4Address;
import java.net.UnknownHostException;

/**
 * This class manages an External Address message. This class is thread-safe. After instantiation, this class may be
 * added to the message queue on the {@link NatPmpDevice}.
 * 
 * @author flszen
 */
public class ExternalAddressRequestMessage
    extends Message
{
    private byte[] payload = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    private Inet4Address externalAddress;

    /**
     * Constructs a new {@link ExternalAddressRequestMessage}.
     * 
     * @param listener The {@link MessageResponseInterface} that will respond to the message result.
     */
    public ExternalAddressRequestMessage( MessageResponseInterface listener )
    {
        super( MessageType.ExternalAddress, listener );
    }

    /**
     * Gets the NAT-PMP gateway's external address.
     * 
     * @return The external {@link Inet4Address} of the NAT-PMP gateway.
     * @throws NatPmpException Thrown when no response has been received or the response parsing ran into some trouble.
     */
    public Inet4Address getExternalAddress()
        throws NatPmpException
    {
        // Return the address.
        return externalAddress;
    }

    void parseResponse( byte[] response )
        throws NatPmpException
    {
        try
        {
            byte[] copy = new byte[4];
            System.arraycopy( response, 8, copy, 0, 4 );
            externalAddress = (Inet4Address) Inet4Address.getByAddress( copy );
        }
        catch ( UnknownHostException ex )
        {
            throw new NatPmpException( "Unable to parse external address.", ex );
        }
    }

    byte[] getRequestPayload()
    {
        return (byte[]) payload.clone();
    }

    byte getOpcode()
    {
        return 0;
    }
}
