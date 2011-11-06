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
 * This interface defines methods that are used for message responses. This
 * interface is implemented by objects that listen for message responses.
 * @author flszen
 */
public interface MessageResponseInterface {
    /**
     * A response was received.
     * @param message The {@link Message} that this notification pertains to.
     */
    void responseReceived(Message message);

    /**
     * No response was received.
     * @param message The {@link Message} that this notification pertains to.
     */
    void noResponseReceived(Message message);

    /**
     * An exception was generated during the message sending process.
     * @param message The {@link Message} that this notification pertains to.
     * @param ex The {@link NatPmpException} that was generated.
     */
    void exceptionGenerated(Message message, NatPmpException ex);
}
