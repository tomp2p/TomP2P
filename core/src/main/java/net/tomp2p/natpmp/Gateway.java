/*
 * Copyright 2012 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.natpmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gateway {
    final private static Logger logger = LoggerFactory.getLogger(Gateway.class);

    final private static Pattern IP_PATTERN = Pattern
            .compile("(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");

    public static InetAddress getIP() {

        // Try to determine the gateway.
        try {
            // Run netstat. This gets the table of routes.
            Process proc = Runtime.getRuntime().exec("netstat -rn");

            InputStream inputstream = proc.getInputStream();
            InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
            BufferedReader bufferedreader = new BufferedReader(inputstreamreader);

            // Parse the result.
            InetAddress inet = parse(bufferedreader);
            bufferedreader.close();
            return inet;
        } catch (IOException ex) {
            logger.error("Unable to determine gateway.", ex);
        }
        return null;
    }

    static InetAddress parse(BufferedReader bufferedreader) throws IOException, UnknownHostException {
        String line;
        while ((line = bufferedreader.readLine()) != null) {
            boolean gatewayLine = false;
            if (line.indexOf("default") == 0 || line.indexOf("0.0.0.0") >= 0) {
                // MacOSX
                if (line.indexOf("default") == 0) {
                    gatewayLine = true;
                }
                // this is the line with the gateway IP, search for the first
                // good entry.
                Matcher m = IP_PATTERN.matcher(line);
                int start = 0;
                while (m.find(start)) {
                    String tmp = m.group();
                    // first entry
                    if (start == 0 && tmp.equals("0.0.0.0")) {
                        gatewayLine = true;
                    } else if (!tmp.equals("0.0.0.0") && gatewayLine) {
                        return InetAddress.getByName(tmp);
                    }
                    start = m.end() + 1;
                }
            }
        }
        return null;
    }
}
