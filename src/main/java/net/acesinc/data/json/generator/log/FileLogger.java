/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.data.json.generator.log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

/**
 *
 * @author andrewserff
 */
public class FileLogger implements EventLogger {

    private static final Logger log = LogManager.getLogger(FileLogger.class);
    public static final String OUTPUT_DIRECTORY_PROP_NAME = "output.directory";
    public static final String FILE_PREFIX_PROP_NAME = "file.prefix";
    public static final String FILE_EXTENSION_PROP_NAME = "file.extension";
    public static final String NUM_OF_LINES = "file.lines";
    public static final String FILE_NAME_PATTERN_PROP_NAME = "file.pattern";
    public static final String DATE_ROLLUP_PROP_NAME = "file.date.rollup";

    private File outputDirectory;
    private String filePrefix;
    private String fileExtension;
    private String fileNamePattern;
    private int numOfLines = -1;
    private boolean genRandomName = false;
    private boolean dateRollup = false;
    private File f;
    private BufferedWriter writer;

    private int counterOfLines = 1;
    private int totalCount = 1;
    private int countForDate = 1;
    private String lastDateAdjustedFileName = "";
    private boolean firstEvent = true;

    public FileLogger(Map<String, Object> props) throws IOException {
        String outputDir = (String) props.get(OUTPUT_DIRECTORY_PROP_NAME);
        outputDirectory = new File(outputDir);
        if (!outputDirectory.exists()) {
            if (!outputDirectory.mkdir()) {
                if (!outputDirectory.mkdirs()) {
                    throw new IOException("Output directory does not exist and we are unable to create it");
                }
            }
        }

        fileNamePattern = (String) props.get(FILE_NAME_PATTERN_PROP_NAME);
        if(StringUtils.isBlank(fileNamePattern)) {
            genRandomName = true;
        }


        filePrefix = (String) props.get(FILE_PREFIX_PROP_NAME);
        filePrefix = filePrefix != null ? filePrefix : "";
        if(genRandomName) {
            filePrefix = StringUtils.rightPad(filePrefix, 3, '_');
        }

        fileExtension = (String) props.get(FILE_EXTENSION_PROP_NAME);
        fileExtension = fileExtension != null ? fileExtension : ".json";

        String numOfLines = (String) props.get(NUM_OF_LINES);
        if(numOfLines != null) {
            this.numOfLines = Integer.valueOf(numOfLines);
        }

        Boolean dateRollup = (Boolean) props.get(DATE_ROLLUP_PROP_NAME);
        if(dateRollup != null) {
            this.dateRollup = dateRollup;
        }

        log.info("Running with: Number of lines: "+this.numOfLines);
    }

    @Override
    public synchronized void logEvent(String event, Map<String, Object> producerConfig) {
        logEvent(event);
    }

    private void logEvent(String event) {
        try {
            createFile();
            writer.write(event+"\n");
            counterOfLines++;
        } catch (IOException ioe) {
            log.error("Unable to create temp file");
        }

    }

    private void createFile() throws IOException {
        if(genRandomName) {
            if(firstEvent || counterOfLines > numOfLines) {
                if(writer != null) {
                    writer.close();
                }
                f = File.createTempFile(filePrefix, fileExtension, outputDirectory);
                writer = new BufferedWriter(new FileWriter(f));
                counterOfLines = 1;
                firstEvent = false;
            }
        } else {
            String fileName = fileNamePattern;

            // Set date fields
            Pattern pattern = Pattern.compile("%t\\{(.+)\\}");
            Matcher matcher = pattern.matcher(fileName);
            Date now = new Date();
            while(matcher.find()) {
                DateFormat format = new SimpleDateFormat(matcher.group(1));
                fileName = fileName.replace(matcher.group(), format.format(now));
            }

            boolean useNewPath = false;
            if(firstEvent) {
                lastDateAdjustedFileName = fileName;
                useNewPath = true;
                firstEvent = false;
            } else {
                if(dateRollup && !fileName.equals(lastDateAdjustedFileName)) {
                    countForDate = 1;
                    totalCount++;
                    counterOfLines = 1;
                    useNewPath = true;
                    lastDateAdjustedFileName = fileName;
                }

                if(numOfLines > 0 && counterOfLines > numOfLines) {
                    counterOfLines = 1;
                    if(!useNewPath) {
                        countForDate++;
                        totalCount++;
                        useNewPath = true;
                    }
                }
            }

            // Set file count
            fileName = fileName.replaceAll("%c", ""+countForDate);
            fileName = fileName.replaceAll("%C", ""+totalCount);

            fileName = filePrefix + fileName + fileExtension;

            if(useNewPath) {
                if(writer != null) {
                    writer.close();
                }
                f = new File(outputDirectory, fileName);
                writer = new BufferedWriter(new FileWriter(f));
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            writer.close();
        } catch (IOException e) {
            log.error(e);
        }
    }

}
