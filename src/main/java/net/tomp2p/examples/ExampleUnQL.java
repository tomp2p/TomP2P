package net.tomp2p.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.examples.Query.QueryType;
import net.tomp2p.examples.Query.ValueType;
import net.tomp2p.examples.json.simple.JSONArray;
import net.tomp2p.examples.json.simple.JSONObject;
import net.tomp2p.examples.json.simple.JSONValue;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class ExampleUnQL
{
    final private static String QUERY_1 = "INSERT INTO abc VALUE 1234;";

    final private static String QUERY_2 = "INSERT INTO abc VALUE 3.141592653;";

    final private static String QUERY_3 = "INSERT INTO abc VALUE \"This is a string\";";

    final private static String QUERY_4 = "INSERT INTO abc VALUE [\"this\",\"is\",\"an\",\"array\"];";

    final private static String QUERY_5 =
        "INSERT INTO abc VALUE { \"type\": \"message\", \"content\": \"This is an object\" };";

    final private static String QUERY_6 = "SELECT FROM abc;";

    public static void main( String[] args )
        throws Exception
    {
        Peer master = null;
        try
        {
            Peer[] peers = ExampleUtils.createAndAttachNodes( 100, 4001 );
            master = peers[0];
            ExampleUtils.bootstrap( peers );
            exampleUnQL( peers );
        }
        finally
        {
            master.shutdown();
        }
    }

    private static void exampleUnQL( Peer[] peers )
        throws IOException, ClassNotFoundException
    {
        execute( peers[22], new Query( QUERY_1 ) );
        execute( peers[23], new Query( QUERY_2 ) );
        execute( peers[24], new Query( QUERY_3 ) );
        execute( peers[25], new Query( QUERY_4 ) );
        execute( peers[26], new Query( QUERY_5 ) );
        execute( peers[27], new Query( QUERY_6 ) );
    }

    public static void execute( Peer peer, String query )
        throws IOException, ClassNotFoundException
    {
        execute( peer, new Query( query ) );
    }

    private static void execute( Peer peer, Query query )
        throws IOException, ClassNotFoundException
    {
        Number160 locationKey = Number160.createHash( query.getCollectionName() );
        if ( query.getQueryType() == QueryType.INSERT )
        {
            if ( query.getValueType() == ValueType.SINGLE )
            {
                peer.add( locationKey ).setData( new Data( query.getValue() ) ).start().awaitUninterruptibly();
            }
            else if ( query.getValueType() == ValueType.ARRAY )
            {
                for ( String value : query.getValues() )
                {
                    peer.add( locationKey ).setData( new Data( value ) ).start().awaitUninterruptibly();
                }
            }
            else if ( query.getValueType() == ValueType.MAP )
            {
                Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
                for ( Map.Entry<String, String> entry : query.getValueMap().entrySet() )
                {
                    dataMap.put( Number160.createHash( entry.getKey() ), new Data( entry.getValue() ) );
                }
                peer.put( locationKey ).setDataMap( dataMap ).start().awaitUninterruptibly();
            }
        }
        else if ( query.getQueryType() == QueryType.SELECT )
        {
            FutureDHT futureDHT = peer.get( locationKey ).setAll().start();
            futureDHT.awaitUninterruptibly();
            for ( Map.Entry<Number160, Data> entry : futureDHT.getDataMap().entrySet() )
            {
                System.out.print( "key: " + entry.getKey() );
                System.out.println( ", value: " + entry.getValue().getObject() );
            }
        }
    }
}

class Query
{
    enum QueryType
    {
        INSERT, SELECT
    };

    enum ValueType
    {
        SINGLE, ARRAY, MAP
    };

    private final QueryType queryType;

    private final String query;

    private final String collectionName;

    private final String value;

    private final List<String> values;

    private final Map<String, String> valueMap;

    private final StringBuilder tmpQuery = new StringBuilder();

    Query( String query )
    {
        this.query = query.trim();
        queryType = parseType();
        collectionName = parseCollectionName();
        Object json = parseJSON();
        value = setSingleValue( json );
        values = setValues( json );
        valueMap = setValueMap( json );
    }

    private QueryType parseType()
    {
        tmpQuery.append( query );
        if ( query.toUpperCase().startsWith( "INSERT INTO" ) )
        {
            tmpQuery.delete( 0, 11 );
            return QueryType.INSERT;
        }
        else if ( query.toUpperCase().startsWith( "SELECT FROM" ) )
        {
            tmpQuery.delete( 0, 11 );
            return QueryType.SELECT;
        }
        else
        {
            throw new IllegalArgumentException( "only insert and select supported" );
        }
    }

    private String parseCollectionName()
    {
        String query = tmpQuery.toString().trim();
        int spacePos = query.indexOf( " " );
        if ( spacePos > 0 )
        {
            tmpQuery.delete( 0, spacePos + 1 );
            return query.substring( 0, spacePos );
        }
        else
        {
            if ( query.endsWith( ";" ) )
            {
                tmpQuery.deleteCharAt( tmpQuery.length() - 1 );
            }
            query = tmpQuery.toString().trim();
            return query;
        }
    }

    private Object parseJSON()
    {
        String query = tmpQuery.toString().trim();
        if ( query.toUpperCase().startsWith( "VALUE" ) )
        {
            tmpQuery.delete( 0, 6 );
        }
        if ( query.endsWith( ";" ) )
        {
            tmpQuery.deleteCharAt( tmpQuery.length() - 1 );
        }
        query = tmpQuery.toString().trim();
        return JSONValue.parse( query );
    }

    private String setSingleValue( Object json )
    {
        if ( json instanceof Long )
        {
            return Long.toString( (Long) json );
        }
        else if ( json instanceof Double )
        {
            return Double.toString( (Double) json );
        }
        else if ( json instanceof String )
        {
            return (String) json;
        }
        return null;
    }

    private List<String> setValues( Object json )
    {
        if ( json instanceof JSONArray )
        {
            List<String> retVal = new ArrayList<String>();
            JSONArray array = (JSONArray) json;
            for ( Object obj : array )
            {
                retVal.add( setSingleValue( obj ) );
            }
            return retVal;
        }
        return null;
    }

    private Map<String, String> setValueMap( Object json )
    {
        if ( json instanceof JSONObject )
        {
            Map<String, String> retVal = new HashMap<String, String>();
            JSONObject map = (JSONObject) json;
            for ( Object key : map.keySet() )
            {
                Object value = map.get( key );
                retVal.put( setSingleValue( key ), setSingleValue( value ) );
            }
            return retVal;
        }
        return null;
    }

    public QueryType getQueryType()
    {
        return queryType;
    }

    public ValueType getValueType()
    {
        if ( getValue() != null )
        {
            return ValueType.SINGLE;
        }
        else if ( getValues() != null )
        {
            return ValueType.ARRAY;
        }
        else
        {
            return ValueType.MAP;
        }
    }

    public String getCollectionName()
    {
        return collectionName;
    }

    public String getValue()
    {
        return value;
    }

    public List<String> getValues()
    {
        return values;
    }

    public Map<String, String> getValueMap()
    {
        return valueMap;
    }
}