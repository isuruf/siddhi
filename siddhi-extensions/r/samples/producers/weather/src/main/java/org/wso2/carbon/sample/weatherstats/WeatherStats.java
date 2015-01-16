/*
 * Copyright (c) 2005-2013, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.sample.weatherstats;

import javax.jms.*;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.om.util.StAXUtils;
import java.util.Random;

public class WeatherStats {
    private static QueueConnectionFactory queueConnectionFactory = null;

    private static final List<String> xmlMsgs = new ArrayList();
    private static String msg;
    private static String part1, part2, part3;
    
      static{
        part1 = "<weatherdata:WeatherStatsStream xmlns:weatherdata=\"http://samples.wso2.org/\">\n" +
                             " <weatherdata:WeatherStat>\n" +
                             " <weatherdata:time>";
                             
        part2 = "</weatherdata:time>\n" +
                             " <weatherdata:temp>";
                             
     	part3 = "</weatherdata:temp>\n" +
                             " </weatherdata:WeatherStat>\n" +
                             " </weatherdata:WeatherStatsStream>";
        
    	}

    public static void main(String[] args) throws XMLStreamException {
	Random random = new Random();
        queueConnectionFactory = JNDIContext.getInstance().getQueueConnectionFactory();
        WeatherStats publisher = new WeatherStats();
        String queueName = "";
        if (args.length == 0 || args[0] == null || args[0].trim().equals("")) {
            queueName = "WeatherStats";
        } else {
            queueName = args[0];
        }

	long nEvents = Long.valueOf(args[1]);
	System.out.println(nEvents);
	int TempVal;
	
	//System.out.println(String.valueOf(val));
	for(long i = 0; i < nEvents; i ++)
	{
		TempVal = random.nextInt(10);
		msg = part1 + String.valueOf(i * 10) + part2 + String.valueOf( i * 5 + TempVal) + part3;
        	xmlMsgs.add(msg);
        }
        publisher.publish(queueName, xmlMsgs);
        System.out.println("All Weather Messages sent");
    }

    /**
     * Publish message to given queue
     *
     * @param queueName - queue name to publish messages
     * @param msgList   - message to send
     */

    public void publish(String queueName, List<String> msgList) throws XMLStreamException {
        // create queue connection
        QueueConnection queueConnection = null;
        try {
            queueConnection = queueConnectionFactory.createQueueConnection();
            queueConnection.start();
        } catch (JMSException e) {
            System.out.println("Can not create queue connection." + e);
            return;
        }
        // create session, producer, message and send message to given destination(queue)
        // OMElement message text is published here.
        Session session = null;
        try {
            session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Sending XML messages on '" + queueName + "' queue");
            for (int i = 0, msgsLength = msgList.size(); i < msgsLength; i++) {
                String xmlMessage = msgList.get(i);
                XMLStreamReader reader = StAXUtils.createXMLStreamReader(new ByteArrayInputStream(
                        xmlMessage.getBytes()));
                StAXOMBuilder builder = new StAXOMBuilder(reader);
                OMElement OMMessage = builder.getDocumentElement();
                TextMessage jmsMessage = session.createTextMessage(OMMessage.toString());
                producer.send(jmsMessage);
                System.out.println("Weather stat " + (i + 1) + " sent");
            }
            producer.close();
            session.close();
            queueConnection.stop();
            queueConnection.close();
        } catch (JMSException e) {
            System.out.println("Can not subscribe." + e);
        }
    }
}
