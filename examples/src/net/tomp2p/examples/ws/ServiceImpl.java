package net.tomp2p.examples.ws;

import java.io.IOException;

import javax.jws.WebService;

import net.tomp2p.examples.ExampleUnQL;

@WebService( endpointInterface = "net.tomp2p.examples.ws.Service" )
public class ServiceImpl
    implements Service
{
    @Override
    public String insert()
    {
        try
        {
            ExampleUnQL.execute( ExampleWSJSON.master, "INSERT INTO abc VALUE 1234;" );
        }
        catch ( ClassNotFoundException e )
        {
            return e.toString();
        }
        catch ( IOException e )
        {
            return e.toString();
        }
        return "ok";
    }

    @Override
    public String query()
    {
        try
        {
            ExampleUnQL.execute( ExampleWSJSON.master, "SELECT FROM abc;" );
        }
        catch ( ClassNotFoundException e )
        {
            return e.toString();
        }
        catch ( IOException e )
        {
            return e.toString();
        }
        return "ok";
    }
}