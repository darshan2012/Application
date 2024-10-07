package com.primary.client;

import com.primary.Constants;
import com.primary.Main;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPClient extends AbstractVerticle
{
    private static final Logger logger = LoggerFactory.getLogger(HTTPClient.class);

    @Override
    public void start()
    {
        vertx.eventBus().<JsonObject>localConsumer(Constants.HTTP_REQUEST, context ->
        {
            request(context.body()).onComplete(response ->
            {
                if (response.succeeded())
                {
                    context.reply(response.result());
                }
                else
                {
                    logger.error("Failed to process HTTP request: {}", response.cause().getMessage());

                    context.fail(500, response.cause().getMessage());
                }
            });
        });
    }


    private Future<JsonObject> request(JsonObject appContext)
    {
        Promise<JsonObject> promise = Promise.promise();

        try
        {
            // Create HTTP client and send a POST request
            vertx.createHttpClient()
                    .request(HttpMethod.POST, appContext.getInteger("port"), appContext.getString("ip"), appContext.getString("endpoint"))
                    .onComplete(asyncResult ->
                    {
                        if (asyncResult.succeeded())
                        {
                            var request = asyncResult.result();

                            request.putHeader("content-type", "application/json");

                            request.idleTimeout(3000);

                            request.send(appContext.encode()).onComplete(requestResult ->
                            {
                                if (requestResult.succeeded())
                                {
                                    var response = requestResult.result();

                                    logger.info("Response status: {}", response.statusCode());

                                    // Check response status
                                    if (response.statusCode() == 200)
                                    {
                                        response.body().onComplete(bodyResult ->
                                        {
                                            if (bodyResult.succeeded())
                                            {
                                                promise.complete(new JsonObject(bodyResult.result()));
                                            }
                                            else
                                            {
                                                logger.error("Failed to read response body: {}", bodyResult.cause()
                                                        .getMessage());

                                                promise.fail(bodyResult.cause());
                                            }
                                        });
                                    }
                                    else
                                    {
                                        String errorMessage = "Request failed with status code: " + response.statusCode();

                                        logger.error(errorMessage);

                                        promise.fail(errorMessage);
                                    }
                                }
                                else
                                {
                                    logger.error("Failed to get a response from server: {}", requestResult.cause()
                                            .getMessage());

                                    promise.fail(requestResult.cause());
                                }
                            });
                        }
                        else
                        {
                            logger.error("Failed to send request to server: {}", asyncResult.cause().getMessage());

                            promise.fail(asyncResult.cause());
                        }
                    });
        }
        catch (Exception exception)
        {
            logger.error("Unexpected exception during HTTP request: {}", exception.getMessage(), exception);

            promise.fail(exception);
        }

        return promise.future();
    }
}
