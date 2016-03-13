package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;
import com.couchbase.org.apache.http.entity.mime.MultipartEntity;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a RemoteRequest with the ability to retry the request
 *
 * Huge caveat: this cannot work on a single threaded requestExecutor,
 * since it blocks while the "subrequests" are in progress, and during the sleeps
 * in between the retries.
 *
 */
public class RemoteRequestRetry<T> implements CustomFuture<T> {

    public static int MAX_RETRIES = 3;  // total number of attempts = 4 (1 initial + MAX_RETRIES)
    public static int RETRY_DELAY_MS = 4 * 1000; // 4 sec

    protected ScheduledExecutorService workExecutor;
    protected ExecutorService requestExecutor;  // must have more than one thread

    // NOTE: this is only required for storing cookie. consider better solution
    protected HttpClientFactory clientFactory;
    protected HttpClient httpClient;
    protected String method;
    protected URL url;
    protected Object body;
    protected Authenticator authenticator;
    protected RemoteRequestCompletionBlock onCompletionCaller;
    protected RemoteRequestCompletionBlock onPreCompletionCaller;

    private int retryCount;
    private Database db;
    protected HttpUriRequest request;

    private AtomicBoolean completedSuccessfully = new AtomicBoolean(false);
    private HttpResponse requestHttpResponse;
    private Object requestResult;
    private Throwable requestThrowable;
    private BlockingQueue<Future> pendingRequests;

    // if true, we wont log any 404 errors (useful when getting remote checkpoint doc)
    private boolean dontLog404;

    protected Map<String, Object> requestHeaders;

    private RemoteRequestType requestType;


    // for Retry task
    ScheduledFuture retryFuture = null;

    private Queue queue = null;

    @Override
    public void setQueue(Queue queue) {
        this.queue = queue;
    }


    /**
     * The kind of RemoteRequest that will be created on each retry attempt
     */
    public enum RemoteRequestType {
        REMOTE_REQUEST,
        REMOTE_MULTIPART_REQUEST,
        REMOTE_MULTIPART_DOWNLOADER_REQUEST
    }

    public RemoteRequestRetry(RemoteRequestType requestType,
                              ScheduledExecutorService requestExecutor,
                              ScheduledExecutorService workExecutor,
                              HttpClientFactory clientFactory,
                              HttpClient httpClient,
                              String method,
                              URL url,
                              Object body,
                              Database db,
                              Map<String, Object> requestHeaders,
                              RemoteRequestCompletionBlock onCompletionCaller) {

        this.requestType = requestType;
        this.requestExecutor = requestExecutor;
        this.clientFactory = clientFactory;
        this.httpClient = httpClient;
        this.method = method;
        this.url = url;
        this.body = body;
        this.onCompletionCaller = onCompletionCaller;
        this.workExecutor = workExecutor;
        this.requestHeaders = requestHeaders;
        this.db = db;
        this.pendingRequests = new LinkedBlockingQueue<Future>();

        validateParameters();

        Log.v(Log.TAG_SYNC, "%s: RemoteRequestRetry created, url: %s", this, url);

    }

    public CustomFuture submit() {
        return submit(false);
    }

    /**
     * @param gzip true - send gzipped request
     */
    public CustomFuture submit(boolean gzip) {

        RemoteRequest request = generateRemoteRequest();

        if (gzip) {
            request.setCompressedRequest(true);
        }

        synchronized (requestExecutor) {
            if (!requestExecutor.isShutdown()) {
                Future future = requestExecutor.submit(request);
                pendingRequests.add(future);
            }
        }

        return this;
    }

    private RemoteRequest generateRemoteRequest() {

        requestHttpResponse = null;
        requestResult = null;
        requestThrowable = null;
        RemoteRequest request = null;

        switch (requestType) {
            case REMOTE_MULTIPART_REQUEST:
                request = new RemoteMultipartRequest(
                        workExecutor,
                        clientFactory,
                        httpClient,
                        method,
                        url,
                        (MultipartEntity) body,
                        requestHeaders,
                        onCompletionInner);
                break;
            case REMOTE_MULTIPART_DOWNLOADER_REQUEST:
                request = new RemoteMultipartDownloaderRequest(
                        workExecutor,
                        clientFactory,
                        httpClient,
                        method,
                        url,
                        body,
                        db,
                        requestHeaders,
                        onCompletionInner);
                break;
            default:
                request = new RemoteRequest(
                        workExecutor,
                        clientFactory,
                        httpClient,
                        method,
                        url,
                        body,
                        requestHeaders,
                        onCompletionInner
                );
                break;
        }

        request.setDontLog404(dontLog404);

        if (this.authenticator != null) {
            request.setAuthenticator(this.authenticator);
        }
        if (this.onPreCompletionCaller != null) {
            request.setOnPreCompletion(this.onPreCompletionCaller);
        }
        return request;
    }

    void removeFromQueue() {
        if (queue != null) {
            queue.remove(this);
            setQueue(null);
        }
    }

    RemoteRequestCompletionBlock onCompletionInner = new RemoteRequestCompletionBlock() {

        private void completed(HttpResponse httpResponse, Object result, Throwable e) {
            requestHttpResponse = httpResponse;
            requestResult = result;
            requestThrowable = e;
            completedSuccessfully.set(true);

            onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);
            
            // release unnecessary references to reduce memory usage as soon as called onComplete().
            requestHttpResponse = null;
            requestResult = null;
            requestThrowable = null;

            removeFromQueue();
        }

        @Override
        public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
            Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry inner request finished, url: %s", this, url);

            if (e == null) {
                Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry was successful, calling callback url: %s", this, url);

                // just propagate completion block call back to the original caller
                completed(httpResponse, result, e);

            } else {

                // Only retry if error is  TransientError (5xx).
                if (isTransientError(httpResponse, e)) {
                    if (requestExecutor != null && requestExecutor.isShutdown()) {
                        // requestExecutor was shutdown, no more retry.
                        Log.e(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, RequestExecutor was shutdown. url: %s", this, url);
                        completed(httpResponse, result, e);
                    }
                    else if (retryCount >= MAX_RETRIES) {
                        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, but transient error.  retries exhausted. url: %s", this, url);
                        // ok, we're out of retries, propagate completion block call
                        completed(httpResponse, result, e);
                    } else {
                        // we're going to try again, so don't call the original caller's
                        // completion block yet.  Eventually it will get called though
                        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, but transient error.  will retry. url: %s", this, url);
                        requestHttpResponse = httpResponse;
                        requestResult = result;
                        requestThrowable = e;
                        retryCount += 1;
                        // delay * 2 << retry
                        long delay = RETRY_DELAY_MS * (long)Math.pow((double)2, (double)Math.min(retryCount-1, MAX_RETRIES));
                        retryFuture = workExecutor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                submit();
                            }
                        }, delay, TimeUnit.MILLISECONDS); // delay init_delay * 2^retry ms
                    }

                } else {
                    Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, non-transient error.  NOT retrying. url: %s", this, url);
                    // this isn't a transient error, so there's no point in retrying
                    completed(httpResponse, result, e);
                }
            }
        }
    };

    private boolean isTransientError(HttpResponse httpResponse, Throwable e) {

        Log.d(Log.TAG_SYNC, "%s: isTransientError called, httpResponse: %s e: %s", this, httpResponse, e);

        if (httpResponse != null) {

            if (Utils.isTransientError(httpResponse.getStatusLine())) {
                Log.d(Log.TAG_SYNC, "%s: its a transient error, return true", this);
                return true;
            }

        } else {
            if (e instanceof IOException) {
                Log.d(Log.TAG_SYNC, "%s: its an ioexception, return true", this);
                return true;
            }

        }

        Log.d(Log.TAG_SYNC, "%s: return false");
        return false;

    }

    /**
     *  Set Authenticator for BASIC Authentication
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void setOnPreCompletionCaller(RemoteRequestCompletionBlock onPreCompletionCaller) {
        this.onPreCompletionCaller = onPreCompletionCaller;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // If RemoteRequestRetry is canceled, make sure if retry future is also canceled.
        if(retryFuture != null && !retryFuture.isCancelled()){
            retryFuture.cancel(mayInterruptIfRunning);
        }

        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {

        while (retryCount <= MAX_RETRIES) {

            // requestExecutor was shutdown, no more retry.
            if (requestExecutor == null || requestExecutor.isShutdown()) {
                return null;
            }

            // Take a future from the queue
            Future future = pendingRequests.take();
            future.get();
            if (completedSuccessfully.get() == true) {
                // we're done
                return null;
            }
        }

        // exhausted attempts, callback to original caller with result.  requestThrowable
        // should contain most recent error that we received.
        // onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);

        return null;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    /**
     * Make sure the user has given us valid parameters
     */
    private void validateParameters() {
        switch (requestType) {
            case REMOTE_MULTIPART_REQUEST:
                if ( !(body instanceof MultipartEntity) ) {
                    throw new IllegalArgumentException("body must be a MultipartEntity for REMOTE_MULTIPART_REQUESTs");
                }
                break;
            default:
                break;

        }
    }

    public void setDontLog404(boolean dontLog404) {
        this.dontLog404 = dontLog404;
    }
}
