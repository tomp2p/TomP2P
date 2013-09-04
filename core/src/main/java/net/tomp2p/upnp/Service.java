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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Representation of an UPNP service
 * 
 * @author <a href="mailto:superbonbon@sbbi.net">SuperBonBon</a>
 * @version 1.0
 */
public class Service {

    /***/
    public final String serviceType;

    /***/
    public final String serviceId;

    /***/
    public final URL SCPDURL;

    /***/
    public final URL controlURL;

    /***/
    public final URL eventSubURL;

    /***/
    public final String USN;

    /***/
    public final Device serviceOwnerDevice;

    private int specVersionMajor;

    private int specVersionMinor;

    private String SCPDURLData;

    private Map<String, Action> UPNPServiceActions = new TreeMap<String, Action>();

    private Map<String, StateVariable> UPNPServiceStateVariables = new TreeMap<String, StateVariable>();

    private boolean parsedSCPD = false;

    /**
     * @param serviceCtx
     * @param baseDeviceURL
     * @param serviceOwnerDevice
     * @throws MalformedURLException
     * @throws XPathExpressionException
     */
    public Service(Node serviceCtx, URL baseDeviceURL, Device serviceOwnerDevice) throws MalformedURLException,
            XPathExpressionException {
        this.serviceOwnerDevice = serviceOwnerDevice;

        serviceType = XMLUtil.xpath.evaluate("serviceType", serviceCtx);
        serviceId = XMLUtil.xpath.evaluate("serviceId", serviceCtx);
        SCPDURL = Device.getURL(XMLUtil.xpath.evaluate("SCPDURL", serviceCtx), baseDeviceURL);
        controlURL = Device.getURL(XMLUtil.xpath.evaluate("controlURL", serviceCtx), baseDeviceURL);
        eventSubURL = Device.getURL(XMLUtil.xpath.evaluate("eventSubURL", serviceCtx), baseDeviceURL);

        USN = serviceOwnerDevice.UDN.concat("::").concat(serviceType);
    }

    /**
     * @return major version
     */
    public int getSpecVersionMajor() {
        lazyInitiate();
        return specVersionMajor;
    }

    /**
     * @return minor version
     */
    public int getSpecVersionMinor() {
        lazyInitiate();
        return specVersionMinor;
    }

    /**
     * Retrieves a service action for its given name
     * 
     * @param actionName
     *            the service action name
     * @return a ServiceAction object or null if no matching action for this
     *         service has been found
     */
    public Action getUPNPServiceAction(String actionName) {
        lazyInitiate();
        return UPNPServiceActions.get(actionName);
    }

    /**
     * Retrieves a service state variable for its given name
     * 
     * @param stateVariableName
     *            the state variable name
     * @return a ServiceStateVariable object or null if no matching state
     *         variable has been found
     */
    public StateVariable getUPNPServiceStateVariable(String stateVariableName) {
        lazyInitiate();
        return UPNPServiceStateVariables.get(stateVariableName);
    }

    /**
     * @return action names
     */
    public Iterator<String> getAvailableActionsName() {
        lazyInitiate();
        return UPNPServiceActions.keySet().iterator();
    }

    /**
     * @return action count
     */
    public int getAvailableActionsSize() {
        lazyInitiate();
        return UPNPServiceActions.keySet().size();
    }

    /**
     * @return state variable names
     */
    public Iterator<String> getAvailableStateVariableName() {
        lazyInitiate();
        return UPNPServiceStateVariables.keySet().iterator();
    }

    /**
     * @return state variable count
     */
    public int getAvailableStateVariableSize() {
        lazyInitiate();
        return UPNPServiceStateVariables.keySet().size();
    }

    private void parseSCPD() {
        try {
            Document doc = XMLUtil.getXML(SCPDURL);
            XPath xpath = XMLUtil.xpath;

            specVersionMajor = Integer.parseInt(xpath.evaluate("scpd/specVersion/major", doc));
            specVersionMinor = Integer.parseInt(xpath.evaluate("scpd/specVersion/major", doc));

            Node varList = (Node) xpath.evaluate("scpd/serviceStateTable", doc, XPathConstants.NODE);
            int varCount = Integer.parseInt(xpath.evaluate("count( stateVariable )", varList));
            UPNPServiceStateVariables = new HashMap<String, StateVariable>();
            for (int i = 1; i <= varCount; i++) {
                Node stateVarXML = (Node) xpath.evaluate("stateVariable[ " + i + " ]", varList, XPathConstants.NODE);
                StateVariable var = new StateVariable(this, stateVarXML);
                UPNPServiceStateVariables.put(var.name, var);
            }

            Node actionList = (Node) xpath.evaluate("scpd/actionList", doc, XPathConstants.NODE);
            int actionCount = Integer.parseInt(xpath.evaluate("count( action )", actionList));
            UPNPServiceActions = new HashMap<String, Action>();
            for (int i = 1; i <= actionCount; i++) {
                Node actionXML = (Node) xpath.evaluate("action[ " + i + " ]", actionList, XPathConstants.NODE);
                Action action = new Action(this, actionXML);

                // hook up the state variables
                for (Argument arg : action.arguments) {
                    arg.relatedStateVariable = UPNPServiceStateVariables.get(arg.relatedStateVariableName);
                }

                UPNPServiceActions.put(action.getName(), action);
            }
            parsedSCPD = true;
        } catch (Throwable t) {
            System.out.println(XMLUtil.getXMLString(SCPDURL));

            throw new RuntimeException("Error during lazy SCDP file parsing at " + SCPDURL, t);
        }
    }

    private void lazyInitiate() {
        if (!parsedSCPD) {
            synchronized (this) {
                if (!parsedSCPD) {
                    parseSCPD();
                }
            }
        }
    }

    /**
     * @return definition xml
     */
    public String getSCDPData() {
        if (SCPDURLData == null) {
            try {
                java.io.InputStream in = SCPDURL.openConnection().getInputStream();
                int readen = 0;
                byte[] buff = new byte[512];
                StringBuilder strBuff = new StringBuilder();
                while ((readen = in.read(buff)) != -1) {
                    strBuff.append(new String(buff, 0, readen));
                }
                in.close();
                SCPDURLData = strBuff.toString();
            } catch (IOException ioEx) {
                return null;
            }
        }
        return SCPDURLData;
    }

    @Override
    public String toString() {
        lazyInitiate();

        StringBuilder b = new StringBuilder();

        b.append("type = ").append(serviceType);
        b.append("\nid = ").append(serviceId);
        b.append("\nurl = ").append(SCPDURL);
        b.append("\ncontrol = ").append(controlURL);
        b.append("\nevent = ").append(eventSubURL);

        b.append("\nActions:");
        for (Action action : UPNPServiceActions.values()) {
            String s = "\n\t" + action.toString();
            s = s.replaceAll("\n", "\n\t");
            b.append(s);
        }
        b.append("\nVariables:");
        for (StateVariable v : UPNPServiceStateVariables.values()) {
            String s = "\n\t" + v.toString();
            s = s.replaceAll("\n", "\n\t");
            b.append(s);
        }

        return b.toString();
    }
}
