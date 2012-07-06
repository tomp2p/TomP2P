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
 * Enumerates the result codes from NAT-PMP messages.
 * 
 * @see Message
 * @author flszen
 */
public enum ResultCode
{
    /**
     * Success: Successful message.
     */
    Success,

    /**
     * Unsupported Version: The version of this client library (which is 0) is not supported by the NAT-PMP gateway.
     */
    UnsupportedVersion,

    /**
     * Not Authorized/Refused: e.g.: Gateway supports mapping, but user has turned feature off.
     */
    NotAuthorizedRefused,

    /**
     * Network Failure: e.g.: The gateway itself has not obtained a DHCP lease.
     */
    NetworkFailure,

    /**
     * Out of resources: Gateway cannot create any more mappings at this time.
     */
    OutOfResources,

    /**
     * Unsupported Opcode: The gateway doesn't support the opcode.
     */
    UnsupportedOpcode
}
