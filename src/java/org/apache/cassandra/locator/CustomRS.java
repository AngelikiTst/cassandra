/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
//Multimap<InetAddress, Token> endpointTokens = HashMultimap.create();
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.dht.Token;


/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the RF nodes that lie right next to each other
 * on the ring.
 */
public class CustomRS extends AbstractReplicationStrategy
{
    protected Map<Token, List<InetAddress>> replicaCache; //=  new HashMap<Token, List<InetAddress>>();
    //protected boolean readFromCache = false;

    public CustomRS(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
        replicaCache = new HashMap<Token, List<InetAddress>>();
        /* Read and load csv file to internal replicaCache */
        try
        {
            replicaCache = readReplicationLocationsFromCsv(tokenMetadata);
            System.out.print(replicaCache.get(0));
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.print("Error during reading csv file\n");
            replicaCache = new HashMap<Token, List<InetAddress>>();
        }
    }

    public HashMap<Token, List<InetAddress>> readReplicationLocationsFromCsv (TokenMetadata tokenMetadata) throws IOException
    {
        HashMap<Token, List<InetAddress>> replicas = new HashMap<Token, List<InetAddress>>();

        String filename = this.keyspaceName + "Replicas.csv";

        File csvFile = new File(filename);

        if (!csvFile.isFile()) {
            return replicas;
        }

        IPartitioner p = tokenMetadata.partitioner; //.getTokenFactory().fromString();

        FileReader filereader = null;

        try
        {
            filereader = new FileReader(filename);
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            System.out.print("No file found\n");
            return replicas;
        }

        BufferedReader csvReader = new BufferedReader(filereader);

        int replicationFactor = getReplicationFactor();

        String row;
        while((row = csvReader.readLine()) != null){
            String[] data = row.split(",");

            String token_str = data[0];

            Token token = p.getTokenFactory().fromString(token_str);
            System.out.print(token);

            List<InetAddress> endpoints = new ArrayList<InetAddress>(replicationFactor);

            for(int i = 1; i < data.length; i++){
                String ip = data[i];
                try
                {
                    InetAddress endpoint = InetAddress.getByName(ip);
                    endpoints.add(endpoint);
                    System.out.print(endpoint);
                }
                catch (UnknownHostException e)
                {
                    System.out.print("Not valid ip\n");
                }
            }

            replicas.put(token, endpoints);
        }
        if (replicas.isEmpty())
        {
            System.out.print("Empty list\n");
        }

        csvReader.close();

        System.out.print(replicas);

        return replicas;
    }

    public void saveReplicationLocationsToCsv(TokenMetadata metadata) throws IOException
    {
        String filename = this.keyspaceName + "Replicas.csv";

        // System.out.print("Trying to save the Replication data!\n");

        IPartitioner p = metadata.partitioner; //.getTokenFactory().fromString();

        FileWriter csvWriter = new FileWriter(filename, false);

        // List<List<String>> outputData = new ArrayList<List<String>>();

        for(Token t : this.replicaCache.keySet())
        {
            List<String> row = new ArrayList<String>();

            // Token.TokenFactory tf = p.getTokenFactory();
            //String tokenStr = p.getTokenFactory().toString(t);
            String tokenStr = Long.toString((Long) t.getTokenValue());
            row.add(tokenStr);

            int size = replicaCache.get(t).size();
            for(int i = 0; i < size; i++){
                // Get endpoint ip to string
                String ip = replicaCache.get(t).get(i).getHostAddress();
                row.add(ip);
            }
            csvWriter.append(String.join(",", row));
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();
    }


    public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata)
    {
        int replicas = getReplicationFactor();
        ArrayList<Token> tokens = metadata.sortedTokens();
        List<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);

        if (tokens.isEmpty())
            return endpoints;

        /* 1st Case: There is an update on tokens => Only first replica-endpoint must change!!
           The rest of the replica-endpoints are not depended on tokens!!
           So if the internal replica cache is not empty and the token is already in the hashmap cache
           set a boolean to update only first replica-endpoint
           If a new token, then calculate as you would normally - first and rest replicas
        */
        boolean update = false;
        if((!replicaCache.isEmpty()) && (replicaCache).containsKey(token))
        {
            update = true;
            System.out.print("Updating first token!!");
            //return replicaCache.get(token);
        }

        // Add the token at the index by default
        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token, false);

        int restOfReplicas = 0;
        if(endpoints.size() < replicas && iter.hasNext())
        {
          InetAddress ep = metadata.getEndpoint(iter.next());
          System.out.println("First Endpoint: " + ep.getHostAddress());
          endpoints.add(ep);
          restOfReplicas = replicas - 1;
        }

        if(update)
        {
             // Update ONLY first endpoint because it depends on the token input!! - The rest are the same!
             replicaCache.get(token).set(0, endpoints.get(0));

             // Reassign endpoint to be the replicaCache for the token
             endpoints = replicaCache.get(token);
        }
        else
        {
            /* This is the DYNAMIC code - must/will be independent of the token ring - i.e. the value of the token!!
            *  For now it depends on the token ring for the ip addresses meaning it takes the endpoint which:
            *  1. The next tokens in the token range - next to the input token - belong to and
            *  2. If the endpoint is not already selected and
            *  3. If its IP ends in even number
            */
            while (endpoints.size() < restOfReplicas && iter.hasNext())
            {
                System.out.println("Searching for endpoints: " + endpoints.size());
                InetAddress ep = metadata.getEndpoint(iter.next());
                String ip = ep.getHostAddress();

                String[] ipSeparated = ip.split("\\.");

                if (Integer.parseInt(ipSeparated[3]) % 2 == 0)
                {
                    if (!endpoints.contains(ep))
                        endpoints.add(ep);
                }
            }
            replicaCache.put(token, endpoints);
        }

        // For every update or calcutation, update / re-write the csv file
        try
        {
            saveReplicationLocationsToCsv(metadata);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.print("Not written!!");
        }
        finally
        {
            if(endpoints.isEmpty()){
                System.out.print("Something went wrong!\n");
            }

            /*
            else {
                System.out.print("Ok I'm returning endpoints now!\n");
            }
            */

            System.out.print(replicaCache);
            return endpoints;
        }
        /*
        try
        {
            endpoints.add(InetAddress.getByName("127.0.0.1")) ;
            System.out.print("Okkk");
        }
        catch (UnknownHostException e)
        {
          System.out.print("Okkk");
        }
        */
    }

    public int getReplicationFactor()
    {
        return Integer.parseInt(this.configOptions.get("replication_factor"));
    }

    public void validateOptions() throws ConfigurationException
    {
        String rf = configOptions.get("replication_factor");
        if (rf == null)
            throw new ConfigurationException("SimpleStrategy2 requires a replication_factor strategy option.");
        validateReplicationFactor(rf);
    }

    public Collection<String> recognizedOptions()
    {
        return Collections.<String>singleton("replication_factor");
    }
}
