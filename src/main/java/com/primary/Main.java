package com.primary;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Main
{
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static Vertx vertx = Vertx.vertx();

    public static Integer port;

    public static Integer pingPort;

    private static final int MAX_EVENTS = 10;

    private static final ZContext context = new ZContext();

    public static void main(String[] args)
    {
        vertx.fileSystem().readFile("config.json").compose(result ->
                {
                    try
                    {
                        JsonObject config = result.toJsonObject();
                        port = config.getInteger("port");
                        pingPort = config.getInteger("pingPort");
                        logger.info("Configuration loaded: {}", config.encodePrettily());
                        return Future.succeededFuture(config);
                    }
                    catch (Exception exception)
                    {
                        logger.error("Failed to load configuration: {}", exception.getMessage());
                        return Future.failedFuture(exception);
                    }
                }).compose(config -> notifyMainServer((JsonObject) config))
                .onComplete(result ->
                {
                    if (result.succeeded())
                    {
                        startEventReceiver();
                    }
                    else
                    {
                        logger.error("Failed to notify main server: {}", result.cause().getMessage());
                    }
                });
    }

    private static Future<Void> notifyMainServer(JsonObject appContext)
    {
        Promise<Void> promise = Promise.promise();
        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/register")
                .onComplete(result ->
                {
                    if (result.succeeded())
                    {
                        HttpClientRequest request = result.result();
                        request.response().onComplete(requestResult ->
                        {
                            if (requestResult.succeeded())
                            {
                                HttpClientResponse response = requestResult.result();
                                logger.info("Response status: {}", response.statusCode());
                                if (response.statusCode() == 200)
                                {
                                    promise.complete();
                                }
                                else
                                {
                                    promise.fail("Could not register, status code: " + response.statusCode());
                                }
                            }
                            else
                            {
                                logger.error("Failed to get response: {}", requestResult.cause().getMessage());
                                promise.fail("Could not register");
                            }
                        });

                        request.putHeader("content-type", "application/json");
                        request.idleTimeout(3000);
                        request.end(appContext.encode());
                    }
                    else
                    {
                        logger.error("Failed to send request to main server: {}", result.cause().getMessage());
                        promise.fail("Could not register");
                    }
                });
        return promise.future();
    }

    private static void startEventReceiver()
    {
        new Thread(() -> {
            try
            {
                ZMQ.Socket pullSocket = context.createSocket(SocketType.PULL);
                pullSocket.bind("tcp://*:" + port); // Bind to a port

                int eventCount = 0;
                StringBuilder eventBuffer = new StringBuilder();

                while (!Thread.currentThread().isInterrupted())
                {
                    String event = pullSocket.recvStr(0);
                    if (event != null)
                    {
                        eventBuffer.append(event).append("\n");
                        eventCount++;

                        if (eventCount >= MAX_EVENTS)
                        {
                            writeEventsToFile(eventBuffer.toString());
                            eventBuffer.setLength(0); // Clear the buffer
                            eventCount = 0; // Reset the event count
                        }
                    }
                }

                pullSocket.close();
            }
            catch (Exception e)
            {
                logger.error("Error in event receiver: ", e);
            }
        }).start();

        new Thread(() -> {
            try
            {
                System.out.println("here");
                ZMQ.Socket pingSocket = context.createSocket(SocketType.REP);
                pingSocket.bind("tcp://*:" + pingPort); // Bind to the pong port

                while (!Thread.currentThread().isInterrupted())
                {
                    // Wait for a ping message
                    String message = pingSocket.recvStr();
                    if ("ping".equals(message))
                    {
                        logger.info("Received " + message + ", sending pong.");
                        pingSocket.send("pong");
                    }
                }
                pingSocket.close();
            }
            catch (Exception e)
            {
                logger.error("Error in ping-pong thread: ", e);
            }
        }).start();
    }

    private static void writeEventsToFile(String events)
    {
        String fileName = "events/event_data_" + System.currentTimeMillis() + ".txt"; // New file name with timestamp
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName)))
        {
            writer.write(events);
            logger.info("Events written to file: {}", fileName);
        }
        catch (IOException e)
        {
            logger.error("Error writing events to file: ", e);
        }
    }
}
