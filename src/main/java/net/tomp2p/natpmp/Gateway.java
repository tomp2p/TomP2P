package net.tomp2p.natpmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gateway 
{
	final private static Logger logger = LoggerFactory.getLogger(Gateway.class);
	
    public static InetAddress getIP()
    {
    	Pattern ipPattern=Pattern.compile("(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");
        // Try to determine the gateway.
        try 
        {
            // Run netstat. This gets the table of routes.
            Process proc = Runtime.getRuntime().exec("netstat -rn");

            InputStream inputstream = proc.getInputStream();
            InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
            BufferedReader bufferedreader = new BufferedReader(inputstreamreader);

            // Parse the result.
            String line;
            while ((line = bufferedreader.readLine()) != null) 
            {
            	if(line.indexOf("default")>=0 || line.indexOf("0.0.0.0")>=0)
            	{
            		//this is the line with the gateway IP, search for the first good entry.
            		Matcher m = ipPattern.matcher(line);
            		int start=0;
            		while(m.find(start))
            		{
            			String tmp=m.group();
            			if(tmp.indexOf("0.0.0.0")<0)
            			{
            				return InetAddress.getByName(tmp);
            			}
            			start = m.end()+1;
            		}
            	}
            }
        } 
        catch (IOException ex) 
        {
        	logger.error("Unable to determine gateway.", ex);
        }
        return null;
    }
}
