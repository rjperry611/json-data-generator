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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	private int numOfLines = 1;
	private boolean genRandomName = false;
	private boolean dateRollup = false;

	private File f;
	private BufferedWriter writer;
	private int counterOfLines = 1;
	private int totalCount = 1;
	private int countForDate = 1;
	private String lastDateAdjustedFileName = "";
	private boolean firstEvent = true;

	public FileLogger() {
		super();
	}

	@Override
	public void setLoggerProps(final Map<String, Object> props) throws IOException {
		String outputDir = (String) props.get(OUTPUT_DIRECTORY_PROP_NAME);
		this.outputDirectory = new File(outputDir);
		if (!this.outputDirectory.exists()) {
			if (!this.outputDirectory.mkdir()) {
				if (!this.outputDirectory.mkdirs()) {
					throw new IOException("Output directory does not exist and we are unable to create it");
				}
			}
		}
		this.fileNamePattern = ((String) props.get(FILE_NAME_PATTERN_PROP_NAME));
		if (StringUtils.isBlank(this.fileNamePattern)) {
			this.genRandomName = true;
		}
		this.filePrefix = ((String) props.get(FILE_PREFIX_PROP_NAME));
		this.filePrefix = (this.filePrefix != null ? this.filePrefix : "");
		if (this.genRandomName) {
			this.filePrefix = StringUtils.rightPad(this.filePrefix, 3, '_');
		}
		this.fileExtension = ((String) props.get(FILE_EXTENSION_PROP_NAME));
		this.fileExtension = (this.fileExtension != null ? this.fileExtension : ".json");
		String numOfLines = (String) props.get(NUM_OF_LINES);
		if (numOfLines != null) {
			this.numOfLines = Integer.valueOf(numOfLines).intValue();
		}
		Boolean dateRollup = (Boolean) props.get(DATE_ROLLUP_PROP_NAME);
		if (dateRollup != null) {
			this.dateRollup = dateRollup.booleanValue();
		}
		log.info("Running with: Number of lines: " + this.numOfLines);
	}

	@Override
	public void logEvent(String event, Map<String, Object> producerConfig) {
		logEvent(event);
	}

	private void logEvent(String event) {
		try {
			createFile();
			this.writer.write(event + "\n");
			this.counterOfLines += 1;
		} catch (IOException ioe) {
			log.error("Unable to create temp file");
		}
	}

	private void createFile() throws IOException {
		if (this.genRandomName) {
			if ((this.firstEvent) || (this.counterOfLines > this.numOfLines)) {
				if (this.writer != null) {
					this.writer.close();
				}
				this.f = File.createTempFile(this.filePrefix, this.fileExtension, this.outputDirectory);
				this.writer = new BufferedWriter(new FileWriter(this.f));
				this.counterOfLines = 1;
				this.firstEvent = false;
			}
		} else {
			String fileName = this.fileNamePattern;

			Pattern pattern = Pattern.compile("%t\\{(.+)\\}");
			Matcher matcher = pattern.matcher(fileName);
			Date now = new Date();
			while (matcher.find()) {
				DateFormat format = new SimpleDateFormat(matcher.group(1));
				fileName = fileName.replace(matcher.group(), format.format(now));
			}
			boolean useNewPath = false;
			if (this.firstEvent) {
				this.lastDateAdjustedFileName = fileName;
				useNewPath = true;
				this.firstEvent = false;
			} else {
				if ((this.dateRollup) && (!fileName.equals(this.lastDateAdjustedFileName))) {
					this.countForDate = 1;
					this.totalCount += 1;
					this.counterOfLines = 1;
					useNewPath = true;
					this.lastDateAdjustedFileName = fileName;
				}
				if ((this.numOfLines > 0) && (this.counterOfLines > this.numOfLines)) {
					this.counterOfLines = 1;
					if (!useNewPath) {
						this.countForDate += 1;
						this.totalCount += 1;
						useNewPath = true;
					}
				}
			}
			fileName = fileName.replaceAll("%c", "" + this.countForDate);
			fileName = fileName.replaceAll("%C", "" + this.totalCount);

			fileName = this.filePrefix + fileName + this.fileExtension;
			if (useNewPath) {
				if (this.writer != null) {
					this.writer.close();
				}
				this.f = new File(this.outputDirectory, fileName);
				this.writer = new BufferedWriter(new FileWriter(this.f));
			}
		}
	}

	@Override
	public void shutdown() {
		try {
			this.writer.close();
		} catch (IOException e) {
			log.error(e);
		}
	}

	@Override
	public String getName() {
		return "file";
	}

}
