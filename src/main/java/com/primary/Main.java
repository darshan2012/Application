package com.primary;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
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

    public static String baseDir;

    private static final ZContext context = new ZContext();

    public static void main(String[] args)
    {
        try
        {
            vertx.fileSystem().readFile("config.json").compose(result ->
                    {
                        try
                        {
                            JsonObject config = result.toJsonObject();

                            baseDir = config.getString("type").toLowerCase();

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
        catch (Exception exception)
        {
            logger.error(exception.getMessage(), exception);
        }
    }

    private static Future<Void> notifyMainServer(JsonObject appContext)
    {
        Promise<Void> promise = Promise.promise();

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/register")
                .onComplete(result ->
                {
                    try
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
                    }
                    catch (Exception exception)
                    {
                        logger.error(exception.getMessage(), exception);
                    }

                });
        return promise.future();
    }

    private static void startEventReceiver()
    {
        new Thread(() ->
        {
            try
            {
                ZMQ.Socket pullSocket = context.createSocket(SocketType.PULL);

                pullSocket.bind("tcp://*:" + port);

                var eventBuffer = new StringBuilder();

                var fileName = "default.txt";

                logger.info("Started listening on port: " + port);

                while (!Thread.currentThread().isInterrupted())
                {
                    var event = pullSocket.recvStr();

                    if (event != null && !event.isEmpty())
                    {
                        logger.info("Received event: " + event);

                        String[] splitEvent = event.split("\\s+", 2);

                        if (splitEvent[0].equals("filename"))
                        {
                            fileName = splitEvent[1];
                        }
                        else if (event.equals("completed"))
                        {
                            writeEvents(fileName, eventBuffer.toString());

                            eventBuffer.setLength(0);
                        }
                        else
                        {
                            eventBuffer.append(event).append("\n");
                        }
                    }
                }

                if (!eventBuffer.isEmpty())
                {
                    writeEvents(fileName, eventBuffer.toString());
                }

                pullSocket.close();

            }
            catch (Exception exception)
            {
                logger.error("Error in event receiver: ", exception);
            }
        }).start();

        new Thread(() ->
        {
            try
            {
                var pingSocket = context.createSocket(SocketType.PUSH);

                pingSocket.bind("tcp://*:" + pingPort);

                while (!Thread.currentThread().isInterrupted())
                {
                    try
                    {
                        pingSocket.send("pong");

                        Thread.sleep(3000);
                    }
                    catch (Exception exception)
                    {
                        logger.error("Error while sending pong :", exception);
                    }
                }
            }
            catch (Exception exception)
            {
                logger.error("Error in ping-pong thread: ", exception);
            }
        }).start();
    }

    private static void writeEvents(String fileName, String eventsBatch)
    {
        vertx.executeBlocking(() ->
        {
            vertx.fileSystem()
                    .open(baseDir + fileName, new OpenOptions().setCreate(true).setAppend(true))
                    .onComplete(result ->
                    {
                        if (result.succeeded())
                        {
                            var file = result.result();

                            file.write(Buffer.buffer(eventsBatch))
                                    .onComplete(writeResult ->
                                    {
                                        if (writeResult.succeeded())
                                        {
                                            logger.info("Flushed events to file: {}", fileName);

                                            file.close();
                                        }
                                        else
                                        {
                                            logger.error("Failed to write events to file: {}", writeResult.cause());
                                        }
                                    });
                        }
                        else
                        {
                            logger.error("Failed to open file for writing: {}", result.cause());
                        }
                    });
            return Future.succeededFuture();
        });

    }
}
