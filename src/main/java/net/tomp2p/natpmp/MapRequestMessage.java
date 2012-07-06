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

/**
 * This class manages an External Address message. This class is thread-safe. After instantiation, this class may be
 * added to the message queue on the {@link NatPmpDevice}. It is important to remove the mapping before shutdown of the
 * {@link NatPmpDevice}. Refer to the parameters of
 * {@link #MapRequestMessage(boolean, int, int, long, com.hoodcomputing.natpmp.MessageResponseInterface) } for details of
 * how to perform this.
 * 
 * @author flszen
 */
public class MapRequestMessage
    extends Message
{
    private int internalPort;

    private int requestedExternalPort;

    private Integer externalPort;

    private int requestedPortMappingLifetime;

    private Integer portMappingLifetime;

    /**
     * Constructs a new {@link MapRequestMessage}. If requestedPortMappingLifetime is zero, an existing port mapping is
     * removed. If both internalPort and requestedPortMappingLifetime are zero, then all mappings for this host are
     * removed.
     * 
     * @param isTCP A value of true indicates that this is a TCP mapping. A value of false indicates that this is a UDP
     *            mapping.
     * @param internalPort The port on which this client is listening. A value of zero, along with a lifetime of zero
     *            indicates that all port mappings for this client are to be removed.
     * @param requestedExternalPort The external port to request. This port may not be available. Always check the
     *            result of the Message to determine which external port is assigned. A value of zero indicates that the
     *            NAT-PMP Gateway should choose a port on its own.
     * @param requestedPortMappingLifetime The time, in seconds, to request the mapping for. A value of zero requests
     *            that this mapping is removed. Always check the result of the Message to determine the actual lifetime
     *            that was assigned, as it may be different than the requested. The recommended port mapping lifetime is
     *            3600 seconds. The protocol specifies an unsigned integer for this parameter, however Java will not
     *            support that as a data type. Therefore, the parameter is given as a long. Do not assign values larger
     *            than an unsigned int can handle to this parameter.
     * @param listener The {@link MessageResponseInterface} that will respond to the message result.
     */
    public MapRequestMessage( boolean isTCP, int internalPort, int requestedExternalPort,
                              int requestedPortMappingLifetime, MessageResponseInterface listener )
    {
        super( isTCP ? MessageType.MapTCP : MessageType.MapUDP, listener );

        // Localize.
        this.internalPort = internalPort;
        this.requestedExternalPort = requestedExternalPort;
        this.requestedPortMappingLifetime = requestedPortMappingLifetime;
    }

    byte[] getRequestPayload()
    {
        byte[] request = new byte[12];
        request[2] = 0; // Reserved
        request[3] = 0; // Reserved
        shortToByteArray( getInternalPort(), request, 4 ); // Internal Port
        shortToByteArray( getRequestedExternalPort(), request, 6 ); // Requested External Port
        intToByteArray( getRequestedPortMappingLifetime(), request, 8 ); // Request Port Mapping Lifetime in Seconds

        return request;
    }

    void parseResponse( byte[] response )
        throws NatPmpException
    {
        int returnedInternalPort = shortFromByteArray( response, 8 );
        if ( returnedInternalPort != internalPort )
        {
            throw new NatPmpException( "The internal port returned from the gateway was not the same as the one sent." );
        }
        internalPort = returnedInternalPort;

        externalPort = shortFromByteArray( response, 10 );
        portMappingLifetime = intFromByteArray( response, 12 );
    }

    byte getOpcode()
    {
        return getMessageType() == MessageType.MapUDP ? (byte) 1 : (byte) 2;
    }

    /**
     * Gets the internal port on which mapped data will arrive.
     * 
     * @return The port on this host that is listening.
     */
    public int getInternalPort()
    {
        return internalPort;
    }

    /**
     * Get sthe external port that was requested.
     * 
     * @return The requested external port.
     */
    public int getRequestedExternalPort()
    {
        return requestedExternalPort;
    }

    /**
     * Gets the external port that was assigned.
     * 
     * @return The external port that was assigned.
     * @throws NatPmpException Thrown if there was an exception generated during the parsing of the respnse.
     */
    public Integer getExternalPort()
        throws NatPmpException
    {
        return externalPort;
    }

    /**
     * Gets the requested port mapping lifetime.
     * 
     * @return The requested port mapping lifetime, in seconds.
     */
    public int getRequestedPortMappingLifetime()
    {
        return requestedPortMappingLifetime;
    }

    /**
     * Gets the assigned port mapping lifetime.
     * 
     * @return The assigned port mapping lifetime, in seconds.
     * @throws NatPmpException Thrown if there was an exception generated during the parsing of the respnse.
     */
    public Integer getPortMappingLifetime()
        throws NatPmpException
    {
        return portMappingLifetime;
    }
}
