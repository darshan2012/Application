package com.primary;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Main
{
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static Vertx vertx = Vertx.vertx();

    public static Integer port;

    public static Integer pingPort;

    public static final String BASE_DIR = System.clearProperty("user.dir") + "/events";

    private static String applicationType;

    private static final ZContext context = new ZContext();

    public static void main(String[] args)
    {
        try
        {
            vertx.fileSystem().readFile("config.json")
                    .compose(result ->
                    {
                        try
                        {
                            JsonObject config = result.toJsonObject();

                            applicationType = config.getString("type").toLowerCase();

                            vertx.fileSystem().mkdirsBlocking(BASE_DIR + "/" + applicationType);

                            port = config.getInteger("port");

                            pingPort = config.getInteger("ping.port");

                            logger.info("Configuration loaded: {}", config.encodePrettily());

                            return Future.succeededFuture(config);
                        }
                        catch (Exception exception)
                        {
                            logger.error("Failed to load configuration: {}", exception.getMessage());

                            return Future.failedFuture(exception);
                        }
                    })
                    .compose(Main::notifyMainServer)
                    .compose(result -> sendHeartBeats())
                    .compose(result -> receiveEvents())
                    .onFailure(result ->
                    {
                        logger.error("Failed to Set up Application", result.getCause());
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
        try
        {
            vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/register")
                    .onComplete(result ->
                    {
                        try
                        {
                            if (result.succeeded())
                            {
                                var request = result.result();

                                request.response().onComplete(requestResult ->
                                {
                                    if (requestResult.succeeded())
                                    {
                                        var response = requestResult.result();

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

                                System.exit(0);
                            }
                        }
                        catch (Exception exception)
                        {
                            logger.error(exception.getMessage(), exception);
                        }

                    });
        }
        catch (Exception exception)
        {
            logger.error(exception.getMessage(), exception);
        }
        return promise.future();
    }

    private static Future<Void> receiveEvents()
    {
        try
        {
            new Thread(() ->
            {
                try
                {
                    ZMQ.Socket pullSocket = context.createSocket(SocketType.PULL);

                    pullSocket.bind("tcp://*:" + port);

                    var eventBuffer = new StringBuilder();

                    var filePath = "default.txt";

                    logger.info("Started listening on port: {}", port);

                    while (!Thread.currentThread().isInterrupted())
                    {
                        var event = pullSocket.recvStr();

                        if (event != null && !event.isEmpty())
                        {
                            logger.info("Received event: {}", event);

                            var splitEvent = event.split("\\s+", 2);

                            if (splitEvent[0].equals("filename"))
                            {
                                filePath = BASE_DIR + "/" + applicationType + "/" + splitEvent[1];

                                logger.info("filename received {} ", splitEvent[1]);

                                if (!eventBuffer.isEmpty())
                                {
                                    writeEvents(filePath, eventBuffer.toString());
                                }

                                String finalFileName = filePath;

                                vertx.executeBlocking(() ->
                                {
                                    if (!vertx.fileSystem().existsBlocking(finalFileName))
                                        vertx.fileSystem().createFileBlocking(finalFileName);

                                    return Future.succeededFuture();
                                });
                            }
                            else if (event.equals("completed"))
                            {
                                writeEvents(filePath, eventBuffer.toString());

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
                        writeEvents(filePath, eventBuffer.toString());
                    }

                    pullSocket.close();

                }
                catch (Exception exception)
                {
                    logger.error("Error in event receiver: ", exception);
                }
            }).start();

            return Future.succeededFuture();
        }
        catch (Exception exception)
        {
            logger.error("Error while starting Event receiver", exception);

            return Future.failedFuture(exception);
        }
    }

    private static void writeEvents(String filePath, String eventsBatch)
    {
        vertx.executeBlocking(() ->
        {
            vertx.fileSystem()
                    .open(filePath,
                            new OpenOptions().setCreate(true).setAppend(true))
                    .onComplete(result ->
                    {
                        try
                        {
                            if (result.succeeded())
                            {
                                var file = result.result();

                                file.write(Buffer.buffer(eventsBatch))
                                        .onComplete(writeResult ->
                                        {
                                            if (writeResult.succeeded())
                                            {
                                                logger.info("Flushed events to file: {}", filePath);

                                                file.close();
                                            }
                                            else
                                            {
                                                logger.error("Failed to write events to file: ", writeResult.cause());
                                            }
                                        });
                            }
                            else
                            {
                                logger.error("Failed to open file for writing: ", result.cause());
                            }
                        }
                        catch (Exception exception)
                        {
                            logger.error(exception.getMessage(), exception);
                        }
                    });
            return Future.succeededFuture();
        });
    }

    private static Future<Void> sendHeartBeats()
    {
        try
        {
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
                            pingSocket.send("I am alive");

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

            return Future.succeededFuture();
        }
        catch (Exception exception)
        {
            logger.error("Error while starting Heartbeat sender", exception);

            return Future.failedFuture(exception);
        }
    }
}
