package com.primary;

import com.primary.client.HTTPClient;
import io.vertx.core.DeploymentOptions;
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

    public static String ip;

    public static Integer port;

    public static Integer pingPort;

    public static JsonObject applicationContext;

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

                            applicationContext = config;

                            logger.info("Configuration loaded: {}", config.encodePrettily());

                            return Future.succeededFuture(config);
                        }
                        catch (Exception exception)
                        {
                            logger.error("Failed to load configuration: {}", exception.getMessage());

                            return Future.failedFuture(exception);
                        }
                    })
                    .compose(result -> vertx.deployVerticle(new HTTPClient()))
                    .compose(result -> vertx.eventBus()
                            .<JsonObject>request(Constants.HTTP_REQUEST, applicationContext.put("endpoint", "/register")))
                    .compose(result ->
                    {
                        port = result.body().getInteger("port");

                        ip = result.body().getString("ip");

                        return Future.succeededFuture();
                    })
                    .compose(result -> sendHeartBeats())
                    .compose(result -> receiveEvents())
                    .onFailure(result ->
                    {
                        logger.error("Failed to Set up Application", result.getCause());

                        vertx.close();
                    });

        }
        catch (Exception exception)
        {
            logger.error(exception.getMessage(), exception);
        }
    }

    private static Future<Void> receiveEvents()
    {
        try
        {
            new Thread(() ->
            {
                try
                {
                    ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);

                    subscriber.connect("tcp://" + ip + ":" + port);

                    subscriber.subscribe(applicationType);

                    var eventBuffer = new StringBuilder();

                    var filePath = "default.txt";

                    logger.info("Started listening on port: {}", port);

                    while (!Thread.currentThread().isInterrupted())
                    {
                        var event = subscriber.recvStr();

                        if (event != null && !event.isEmpty())
                        {
                            logger.info("Received event: {}", event);

                            var splitEvent = event.split("\\s+", 2);

                            if (splitEvent.length < 2)
                            {
                                continue;
                            }

                            if (splitEvent[1].startsWith("filename"))
                            {
                                filePath = BASE_DIR + "/" + applicationType + "/" + splitEvent[1].split("\\s+", 2)[1];

                                logger.info("filename received {} ", filePath);

                                if (!eventBuffer.isEmpty())
                                {
                                    writeEvents(filePath, eventBuffer.toString());

                                    eventBuffer.setLength(0);
                                }

                                String finalFileName = filePath;

                                vertx.executeBlocking(() ->
                                {
                                    if (!vertx.fileSystem().existsBlocking(finalFileName))
                                        vertx.fileSystem().createFileBlocking(finalFileName);

                                    return Future.succeededFuture();
                                });
                            }
                            else if (splitEvent[1].startsWith("completed"))
                            {
                                writeEvents(filePath, eventBuffer.toString());

                                eventBuffer.setLength(0);
                            }
                            else
                            {
                                eventBuffer.append(splitEvent[1]).append("\n");
                            }
                        }
                    }
                    if (!eventBuffer.isEmpty())
                    {
                        writeEvents(filePath, eventBuffer.toString());

                        eventBuffer.setLength(0);
                    }

                    subscriber.close();

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
                            pingSocket.send("I am alive", ZMQ.DONTWAIT);

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
