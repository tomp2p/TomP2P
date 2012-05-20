package net.tomp2p.examples.ws;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.jws.soap.SOAPBinding.Style;

@WebService     // This signals that this is a Service Endpoint Interface (SEI) 
@SOAPBinding(style = Style.RPC) // Needed for the WSDL 
public interface Service
{
	@WebMethod  // This signals that this method is a service operaNon 
	public String insert();
	
	@WebMethod   // This signals that this method is a service operaNon
	public String query();
}

