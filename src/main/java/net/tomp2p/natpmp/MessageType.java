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
 * This enumeration enumerates the types of messages that can be sent. This is
 * used mostly my the {@link Message} class.
 * @author flszen
 */
enum MessageType {
    /**
     * Requests the external address of the NAT-PMP device.
     */
    ExternalAddress,

    /**
     * Requests a TCP port map on the NAT-PMP device. To remove a mapping, set
     * the lifetime and external port set to 0. If the internal port is also set
     * to zero, than all mappings are removed.
     */
    MapTCP,

    /**
     * Requests a UDP port map on the NAT-PMP device. To remove a mapping, set
     * the lifetime and external port set to 0. If the internal port is also set
     * to zero, than all mappings are removed.
     */
    MapUDP
}
