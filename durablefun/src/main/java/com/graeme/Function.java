package com.graeme;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;
import io.reactivex.rxjava3.core.Observable;

/**
 * Please follow the below steps to run this durable function sample
 * 1. Send an HTTP GET/POST request to endpoint `StartHelloCities` to run a durable function
 * 2. Send request to statusQueryGetUri in `StartHelloCities` response to get the status of durable function
 * For more instructions, please refer https://aka.ms/durable-function-java
 * 
 * Please add com.microsoft:durabletask-azure-functions to your project dependencies
 * Please add `"extensions": { "durableTask": { "hubName": "JavaTestHub" }}` to your host.json
 */
public class Function {
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("StartOrchestration")
    @StorageAccount("AzureWebJobsStorage")
    public HttpResponseMessage startOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BlobOutput(name="test", path="input-files/test.json") OutputBinding<String> output,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();

        String fileName = request.getQueryParameters().get("filename");

        //simulate the process by adding a large file - 10,000 rows in a JSON file
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<BatchItem> items = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            BatchItem item = new BatchItem();
            item.setItemId(i);
            item.setName("test item " + i);
            items.add(item);
        }
        try {
            output.setValue(mapper.writeValueAsString(items));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        String instanceId = client.scheduleNewOrchestrationInstance("ProcessHugeFile", "input-files/test.json");

        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);

        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * This is the orchestrator function, which can schedule activity functions, create durable timers,
     * or wait for external events in a way that's completely fault-tolerant.
     */
    @FunctionName("ProcessHugeFile")
    public boolean hugeFileProcessor(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {

        String fileName = ctx.getInput(String.class);

        //we don't want the activity to return each item in the file as it's too much state. The files are huge.
        //Instead, let's count the number of rows, then start splitting the file into smaller processing units.
        int expectedCount = ctx.callActivity("countItemsInFile", fileName, int.class).await();

        //First split operation is given the entirity of the file
        FileSplitInput input = new FileSplitInput();
        input.setIndexStart(0);
        input.setIndexEnd(expectedCount);
        int processedCount = ctx.callSubOrchestrator("SplitAndProcess", input, int.class).await();

        //Reconciliation logic. Did we process as much as we expected?
        return expectedCount == processedCount;

    }


    /**
     * This is the activity function that gets invoked by the orchestration.
     */
    @FunctionName("countItemsInFile")
    public int countItemsInFile(
            @DurableActivityTrigger(name = "filename") String filename,
            final ExecutionContext context) {

        //count the items using jackson to avoid loading a huge byte array
        AtomicInteger count = new AtomicInteger();

        //Stream the JSON so we don't pull it all into memory.
        streamJsonFile()
                .subscribe(
                        item -> count.getAndIncrement(),
                        throwable -> System.err.println("Error: " + throwable.getMessage()),
                        () -> System.out.println("All items read:" + count.get())
                );
        return count.get();
    }

    @FunctionName("SplitAndProcess")
    public int splitAndProcess(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {

        FileSplitInput fileSplitInput = ctx.getInput(FileSplitInput.class);
        int itemsInBatch = fileSplitInput.getIndexEnd() - fileSplitInput.getIndexStart();

        List<Task<Integer>> batches = new ArrayList<>();

        //Check if we have got to a small amount of items. If so, process them as a sub-orchestration
        if (itemsInBatch < 25) {
            MiniBatchInput input = new MiniBatchInput();
            input.setBatchStart(fileSplitInput.getIndexStart());
            input.setBatchEnd(fileSplitInput.getIndexEnd());
            batches.add(ctx.callSubOrchestrator("ProcessMiniBatch", input, int.class));
        } else {

            //Split the batch in half, and call the SplitAndProcess function again. This is a sub-orchestration so each orchestration will only end up tracking 2 sub-orchestrations.
            //Again this will reduce the state passed around in a single Orchestrator
            int batchMidway = Math.round((float)itemsInBatch / 2);
            FileSplitInput input1 = new FileSplitInput();
            input1.setIndexStart(fileSplitInput.getIndexStart());
            input1.setIndexEnd(fileSplitInput.getIndexStart() + batchMidway);
            batches.add(ctx.callSubOrchestrator("SplitAndProcess", input1, int.class));

            FileSplitInput input2 = new FileSplitInput();
            input2.setIndexStart(fileSplitInput.getIndexStart() + batchMidway + 1);
            input2.setIndexEnd(fileSplitInput.getIndexEnd());
            batches.add(ctx.callSubOrchestrator("SplitAndProcess", input2, int.class));

        }

        List<Integer> results = ctx.allOf(batches).await();

        return results.stream().mapToInt(Integer::intValue).sum();
    }


    public static Observable<BatchItem> streamJsonFile() {
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory());

        // Azure SDK client builders accept the credential as a parameter
        BlobServiceClient blobServiceClient =  new BlobServiceClientBuilder()
                .connectionString(System.getenv("AzureWebJobsStorage"))
                .buildClient();

        BlobClient blobClient = blobServiceClient.getBlobContainerClient("input-files").getBlobClient("test.json");

        return Observable.create(emitter -> {
            try {
                JsonNode rootNode = objectMapper.readTree(blobClient.openInputStream());
                Iterator<JsonNode> iterator = rootNode.elements();

                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    BatchItem item = objectMapper.treeToValue(node, BatchItem.class);
                    emitter.onNext(item);
                }
                emitter.onComplete();
            } catch (IOException e) {
                emitter.onError(e);
            }
        });
    }


    @FunctionName("ProcessMiniBatch")
    public int processMiniBatch(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx,
            ExecutionContext context) {

        var batchInformation = ctx.getInput(MiniBatchInput.class);
        List<Task<Boolean>> processAll = new ArrayList<>();

        //Mini batches are only 20 or so items. So lets just get all the items, and process them.
        BatchItem[] items = ctx.callActivity("GetMiniBatchItems", batchInformation, BatchItem[].class).await();

        for (BatchItem item : items) {
            //Could be an activity but in-case the processing is complex, lets use a sub-orchestration
            processAll.add(ctx.callSubOrchestrator("ProcessItemOrchestration", item, Boolean.class));
        }

        List<Boolean> allResults = ctx.allOf(processAll).await();

        return allResults.size();

    }

    @FunctionName("GetMiniBatchItems")
    public BatchItem[] getMiniBatchItems(
            @DurableActivityTrigger(name = "input") MiniBatchInput input) {

        List<BatchItem> items = new ArrayList<>();
        AtomicInteger count = new AtomicInteger();
        //This is a little bit wasteful as we read through the file in storage quite a bit. Another approach would be to constantly split the file into smaller new files
        //as we do the initial chunking. That would be costlier up-front, but more efficient when we attempt to pull out batch items.
        streamJsonFile()
                .subscribe(
                        item -> {
                            int index = count.getAndIncrement();
                            if (index >= input.getBatchStart() && index <= input.getBatchEnd()) {
                                items.add(item);
                            }
                        },
                        throwable -> System.err.println("Error: " + throwable.getMessage()),
                        () -> System.out.println("All items read:" + count.get())
                );

        return items.toArray(new BatchItem[0]);
    }

    @FunctionName("ProcessItemOrchestration")
    public boolean processItemOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        return ctx.callActivity("ProcessItem", ctx.getInput(BatchItem.class), Boolean.class).await();
    }

    @FunctionName("ProcessItem")
    public boolean processItem(
            @DurableActivityTrigger(name = "input") BatchItem input) {

        //write a small file out to proove this was processed
        BlobServiceClient blobServiceClient =  new BlobServiceClientBuilder()
                .connectionString(System.getenv("AzureWebJobsStorage"))
                .buildClient();

        //Just an example activity - adds a new file to storage with the contens of the mini batch item.
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("output-files");
        blobContainerClient.createIfNotExists();
        BlobClient blobClient = blobContainerClient.getBlobClient( "processed-" + input.getItemId() + ".json");

        try {
            blobClient.upload(new ByteArrayInputStream(new ObjectMapper().writeValueAsString(input).getBytes(StandardCharsets.UTF_8)));
        } catch (JsonProcessingException e) {
        }

        return true;
    }

}

