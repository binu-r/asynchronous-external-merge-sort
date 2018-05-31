package com.binu.external.asynchronous.merge.sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class ExternalAsynchronousSortUtil {
    private List<File> files=new ArrayList<>();
    final private String outputFile;
    final private String tempDirectory;
    final private String inputFileName;
    final private Comparator<String> comparator;
    private ThreadPoolExecutor threadPoolExecutor=(ThreadPoolExecutor) Executors.newCachedThreadPool();

    private ExternalAsynchronousSortUtil(String inputFileName, String tempDirectory, String outputFile, Comparator<String> comparator) {
        this.outputFile = outputFile;
        this.tempDirectory = tempDirectory;
        this.inputFileName = inputFileName;
        this.comparator = comparator;
    }

    public static void performSort(final String input,final String output,final String temporarySortDirectory,
                                   final Comparator<String> comparator) throws Exception{
        ExternalAsynchronousSortUtil externalAsynchronousSortUtil=new ExternalAsynchronousSortUtil(input, temporarySortDirectory, output, comparator);
        externalAsynchronousSortUtil.sort();
    }


    private void sort() throws Exception {
        Future<?> future = threadPoolExecutor.submit(new Runnable() {

            @Override
            public void run() {
                boolean initialized = false;
                List<String> inputLines = new ArrayList<>();
                int averageNumberOfLinesPerFile = 10000;
                int i = 0;
                File inputFile = new File(inputFileName);
                try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(inputFile));
                     BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

                    String input;
                    while ((input = bufferedReader.readLine()) != null) {
                        if (!initialized) {
                            initialized = true;
                        }
                        inputLines.add(input);
                        i++;
                        if (i >= averageNumberOfLinesPerFile) {
                            i = 0;

                            files.add(createTempFile(inputLines));
                            inputLines = new ArrayList<>();
                        }

                    }
                    if (!inputLines.isEmpty()) {
                        files.add(createTempFile(inputLines));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                threadPoolExecutor.shutdown();
            }
        });
        future.get();
        performMerge();
    }

    private void performMerge() throws Exception {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile))) {
            List<Buffer> buffers = new ArrayList<>();
            for (File file : files) {
                buffers.add(new Buffer(file));
            }
            PriorityQueue<Buffer> priorityQueue = new PriorityQueue<>(files.size(), new Comparator<Buffer>() {
                @Override
                public int compare(Buffer i, Buffer j) {
                    return comparator.compare(i.getCurrentLine(), j.getCurrentLine());
                }
            });

            for (Buffer buffer : buffers) {
                Collections.addAll(priorityQueue,buffer);
            }
            while (!priorityQueue.isEmpty()) {
                Buffer buffer = priorityQueue.poll();
                String r = buffer.selectCurrentLine();
                bufferedWriter.write(r);
                bufferedWriter.newLine();
                if (buffer.isEmpty()) {
                    buffer.close();
                } else {
                    priorityQueue.add(buffer);
                }
            }
        }

    }


    private File createTempFile(List<String> inputLines) {
        try {
            File tempFile=File.createTempFile("temp", "sort", new File(tempDirectory));
            threadPoolExecutor.execute(new SortTask(tempFile, inputLines));
            tempFile.deleteOnExit();
            return tempFile;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    private static class Buffer {
        BufferedReader bufferedReader;
        String currentBuffer;
        Buffer(File input) throws Exception{
            bufferedReader=new BufferedReader(new FileReader(input));
            readLine();
        }

        String getCurrentLine(){
            return currentBuffer;
        }
        String selectCurrentLine() throws Exception{
            String currentLine=currentBuffer;
            readLine();
            return currentLine;
        }
        private void readLine() throws Exception{
            currentBuffer=bufferedReader.readLine();
        }
        boolean isEmpty(){
            return null==currentBuffer;
        }
        void close() throws Exception{
            bufferedReader.close();
        }
    }


    class SortTask implements Runnable {
        private final List<String> inputLines;
        private final File tempFile;

        SortTask(final File tempFile, final List<String> inputLines) {
            this.tempFile = tempFile;
            this.inputLines = inputLines;
        }

        @Override
        public void run() {
            Collections.sort(inputLines, comparator);
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tempFile, true))) {
                for (String line : inputLines) {
                    bufferedWriter.write(line);
                    bufferedWriter.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
