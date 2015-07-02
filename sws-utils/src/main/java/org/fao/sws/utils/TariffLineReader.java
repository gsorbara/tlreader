package org.fao.sws.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TariffLineReader {
	
	public static void main(String[] args) {
		String logsKey = (new SimpleDateFormat("yyyyMMdd_hhmmss")).format(new Date());
		try (
				FileOutputStream logFos = new FileOutputStream(new File(args[3] + File.separator + "log_tariffline_" + logsKey + ".log"));
				FileOutputStream complFos = new FileOutputStream(new File(args[3] + File.separator + "completed_tariffline_" + logsKey + ".log"));
				FileOutputStream errFos = new FileOutputStream(new File(args[3] + File.separator + "error_tariffline_" + logsKey + ".log"));
			) {
			TariffLineReader.readMany(args[0], args[1], Integer.parseInt(args[2]), args[3], logFos, complFos, errFos);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static void readManyWCommodity(String countriesList, String yearsList, String commodiyFilter, int threads, String outFolder, OutputStream loggerStream, OutputStream completedStream, OutputStream errorStream) {
		try {
			List<ReaderThread> readersPool = new ArrayList<ReaderThread>();
			List<ExecutionReq> executionReqQueue = new ArrayList<ExecutionReq>();
			String[] years = yearsList.split("[,]"), countries = countriesList.split("[,]");
			for (String year : years) {
				for (String country : countries) {
					executionReqQueue.add(new ExecutionReq(Integer.parseInt(country), Integer.parseInt(year), commodiyFilter, outFolder, loggerStream, completedStream, errorStream));
				}
			}
			for (int i = 0; i < threads; i++) {
				readersPool.add(new ReaderThread(executionReqQueue, (i + 1)));
				readersPool.get(i).start();
			}
			for (int i = 0; i < threads; i++) {
				readersPool.get(i).join();
			}
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}
	
	public static void readMany(String countriesList, String yearsList, int threads, String outFolder, OutputStream loggerStream, OutputStream completedStream, OutputStream errorStream) {
		try {
			List<ReaderThread> readersPool = new ArrayList<ReaderThread>();
			List<ExecutionReq> executionReqQueue = new ArrayList<ExecutionReq>();
			String[] years = yearsList.split("[,]"), countries = countriesList.split("[,]");
			for (String year : years) {
				for (String country : countries) {
					executionReqQueue.add(new ExecutionReq(Integer.parseInt(country), Integer.parseInt(year), outFolder, loggerStream, completedStream, errorStream));
				}
				
			}
			for (int i = 0; i < threads; i++) {
				readersPool.add(new ReaderThread(executionReqQueue, (i + 1)));
				readersPool.get(i).start();
			}
			for (int i = 0; i < threads; i++) {
				readersPool.get(i).join();
			}
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}
	
	public static void readOne(int country, int year, String outFolder, OutputStream loggerStream, OutputStream completedStream, OutputStream errorStream) {
		try {
			ExecutionReq executionReq = new ExecutionReq(country, year, outFolder, loggerStream, completedStream, errorStream);
			List<ExecutionReq> executionReqQueue = new ArrayList<ExecutionReq>();
			executionReqQueue.add(executionReq);
			new ReaderThread(executionReqQueue, 1).readNext();
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}
	
	public static class ExecutionReq {
		public static String BASE_URL = "http://comtrade.un.org/ws/getSdmxTariffLineV1.aspx?r=%d&y=%d&cc=*&comp=false";
		public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		int country;
		int year;
		String commodityFilter;
		URL url;
		File outFile;
		long startedAt;
		long endedAt;
		Boolean started;
		Boolean ended;
		List<byte[]> chunks;
		OutputStream logStream;
		OutputStream completedStream;
		OutputStream errorStream;
		
		public ExecutionReq(int country, int year, String outFolder, OutputStream logStream, OutputStream completedStream, OutputStream errorStream) throws Exception {
			outFolder = outFolder.endsWith("/") || outFolder.endsWith("\\") ? outFolder = outFolder.substring(0, outFolder.length() - 1) : outFolder;
			if (File.separatorChar != '\\')
				outFolder = outFolder.replace('\\', File.separatorChar);
			this.country = country;
			this.year = year;
			this.url = new URL(String.format(BASE_URL, country, year));
			this.outFile = new File(outFolder + File.separator + "tariffline_c" + country + "_y" + year + ".csv");
			this.logStream = logStream;
			this.completedStream = completedStream;
			this.errorStream = errorStream;
			this.started = false;
			this.ended = false;
			this.chunks = new ArrayList<byte[]>();
		}

		public ExecutionReq(int country, int year, String commodityFilter, String outFolder, OutputStream logStream, OutputStream completedStream, OutputStream errorStream) throws Exception {
			outFolder = outFolder.endsWith("/") || outFolder.endsWith("\\") ? outFolder = outFolder.substring(0, outFolder.length() - 1) : outFolder;
			if (File.separatorChar != '\\')
				outFolder = outFolder.replace('\\', File.separatorChar);
			this.country = country;
			this.year = year;
			this.commodityFilter = commodityFilter;
			this.url = new URL(String.format(BASE_URL, country, year));
			this.outFile = new File(outFolder + File.separator + "tariffline_c" + country + "_y" + year + ".csv");
			this.logStream = logStream;
			this.completedStream = completedStream;
			this.errorStream = errorStream;
			this.started = false;
			this.ended = false;
			this.chunks = new ArrayList<byte[]>();
		}
		
		public void log(int threadNumber, int cntExecutions, String msg) throws Exception {
			if (logStream != null) logStream.write((getCurrExecLabel(threadNumber, cntExecutions) + " - " + msg + "\n").getBytes());
		}
		public void log(int threadNumber, int cntExecutions, String msg, Exception ex) throws Exception {
			if (logStream != null) {
				StringWriter writer = new StringWriter();
				ex.printStackTrace(new PrintWriter(writer));
				logStream.write((getCurrExecLabel(threadNumber, cntExecutions) + " - " + msg + "\n" + writer.toString() + "\n").getBytes());
			}
		}
		public String getCurrExecLabel(int threadNumber, int cntExecutions) {
			return SDF.format(new Date())  + " - [reader#" + threadNumber + "(" + cntExecutions + ",c=" + country + ",y=" + year + ")]";
		}
		public void logCompleted(int threadNumber, int cntExecutions) throws Exception {
			if (completedStream != null) completedStream.write((country + "\t" + year + "\t" + outFile.getName() + "\n").getBytes());
		}
		public void logError(int threadNumber, int cntExecutions) throws Exception {
			if (errorStream != null) errorStream.write((country + "\t" + year + "\t" + outFile.getName() + "\n").getBytes());
		}
	}
	
	public static class ReaderThread extends Thread {
		
		public ReaderThread(List<ExecutionReq> executionReqQueue, int threadNumber) {
			this.executionReqQueue = executionReqQueue;
			this.threadNumber = threadNumber;
			this.cntExecutions = 0;
		}
		List<ExecutionReq> executionReqQueue;
		int threadNumber;
		int cntExecutions;
		
		@Override
		public void run() {
			try {
				while (readNext());
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		
		public boolean readNext() {
			ExecutionReq executionReq = null;
			this.cntExecutions++;
			try {
				synchronized (executionReqQueue) {
					if (executionReqQueue.size() > 0) {
						executionReq = executionReqQueue.get(0);
						executionReqQueue.remove(0);
					}
				}
				if (executionReq != null) {
					executionReq.log(this.threadNumber, this.cntExecutions, "started");

					executionReq.startedAt = System.currentTimeMillis();
					Downloader downloader = new Downloader(executionReq, this.threadNumber, this.cntExecutions);
					Parser parser = new Parser(executionReq, this.threadNumber, this.cntExecutions);

					downloader.start();
					parser.start();

					parser.join();
					executionReq.endedAt = System.currentTimeMillis();
					executionReq.log(this.threadNumber, this.cntExecutions, "completed OK in " + ((int) ((executionReq.endedAt - executionReq.startedAt) / 1000)) + " seconds");
					executionReq.logCompleted(this.threadNumber, this.cntExecutions);
				}
			} catch (Exception ex) {
				if (executionReq != null) {
					try {
						executionReq.log(this.threadNumber, this.cntExecutions, "an error occurred:" + ex);
						if (executionReq.outFile != null && executionReq.outFile.exists()) executionReq.outFile.delete();
					} catch (Exception e2) {}
				} else {
					ex.printStackTrace();
				}
			}
			return executionReq != null;
		}
	}

	public static class Downloader extends Thread {
		ExecutionReq executionReq;
		int threadNumber;
		int cntExecutions;
		public Downloader(ExecutionReq executionReq, int threadNumber, int cntExecutions) {
			this.executionReq = executionReq;
			this.threadNumber = threadNumber;
			this.cntExecutions = cntExecutions;
		}
		
		@Override
		public void run() {
			try (InputStream is = executionReq.url.openStream()) {
				synchronized (executionReq.started) {
					executionReq.started = true;
				}
				executionReq.log(threadNumber, cntExecutions, "downloader - download started");
				
				byte[] chunk = new byte[1024 + 5];
			    int chunkLen = 0, cnt = 0;
			    while ((chunkLen = is.read(chunk)) != -1) {
					push(chunk, chunkLen);
					if (++cnt % 5000 == 0) {
						executionReq.log(threadNumber, cntExecutions, "downloader - chunk added " + (cnt));
					}
			    }
				synchronized (executionReq.ended) {
					executionReq.ended = true;
				}
				executionReq.log(threadNumber, cntExecutions, "downloader - download ended");
			} catch (Exception ex) {
				synchronized (executionReq.ended) {
					executionReq.ended = true;
				}
				throw new RuntimeException(ex);
			}
			try {
				executionReq.log(threadNumber, cntExecutions, "downloader - thread ended");
			} catch (Exception e) {}
		}
		
		private void push(byte[] src, int len) {
			if (src.length > 0) {
				byte[] copied = new byte[len];
				System.arraycopy(src, 0, copied, 0, len);
				synchronized (executionReq.chunks) {
					executionReq.chunks.add(copied);
				}
			}
		}
	}
	
	public static class Parser extends Thread {
		ExecutionReq executionReq;
		int threadNumber;
		int cntExecutions;
		public Parser(ExecutionReq executionReq, int threadNumber, int cntExecutions) {
			this.executionReq = executionReq;
			this.threadNumber = threadNumber;
			this.cntExecutions = cntExecutions;
		}
		
		public static String parseVal(String attr) {
			String out = attr.substring(attr.indexOf("\""));
			return out.substring(1, out.length() - 1);
		}

		public class GroupRow {
			GroupRow(String raw) {
				rpt = "";
				time = "";
				currency = "";
				repClass = "";
				String[] fields = raw.split(" ");
				for (String field : fields) {
					if (field.startsWith("RPT")) rpt = parseVal(field);
					else if (field.startsWith("time")) time = parseVal(field);
					else if (field.startsWith("CURRENCY")) currency = parseVal(field);
					else if (field.startsWith("REPORTED_CLASSIFICATION")) repClass = parseVal(field);
				}
			}
			String rpt;
			String time;
			String currency;
			String repClass;
			
			String toCsv() {
				return rpt + "\t" + time + "\t" + currency + "\t" + repClass;
			}
		}
		public class SectRow {
			SectRow(String raw) {
				tf = "";
				repCurr = "";
				String[] fields = raw.split(" ");
				for (String field : fields) {
					if (field.startsWith("TF")) tf = parseVal(field);
					else if (field.startsWith("REPORTED_CURRENCY")) repCurr = parseVal(field);
				}
			}
			String tf;
			String repCurr;

			String toCsv() {
				return tf + "\t" + repCurr;
			}
		}
		public class ObsRow {
			ObsRow(String raw) {
				cc = "";
				prt = "";
				netweight = "";
				qty = "";
				value = "";
				est = "";
				ht = "";
				String[] fields = raw.split(" ");
				for (String field : fields) {
					if (field.startsWith("CC-H")) cc = parseVal(field);
					else if (field.startsWith("PRT")) prt = parseVal(field);
					else if (field.startsWith("netweight")) netweight = parseVal(field);
					else if (field.startsWith("qty")) qty = parseVal(field);
					else if (field.startsWith("QU")) qu = parseVal(field);
					else if (field.startsWith("value")) value = parseVal(field);
					else if (field.startsWith("EST")) est = parseVal(field);
					else if (field.startsWith("HT")) ht = parseVal(field);
				}
			}
			String cc;
			String prt;
			String netweight;
			String qty;
			String qu;
			String value;
			String est;
			String ht;
			
			String toCsv() {
				return cc + "\t" + prt + "\t" + netweight + "\t" + qty + "\t" + qu + "\t" + value + "\t" + est + "\t" + ht;
			}
		}

		@Override
		public void run() {
			try (FileOutputStream fos = new FileOutputStream(executionReq.outFile)) {
				executionReq.log(this.threadNumber, this.cntExecutions, "parser - thread running");
				while (!executionReq.started) {
					Thread.sleep(10);
				}
				executionReq.log(this.threadNumber, this.cntExecutions, "parser - start detected");
				int cnt = 0;
				String buffer = "";
				boolean contentStarted = false, contentEnded = false;
				GroupRow groupRow = null;
				SectRow sectRow = null;
				
				while (!executionReq.ended || executionReq.chunks.size() > 0) {
					if (executionReq.chunks.size() == 0) {
						Thread.sleep(100);
					}
					byte[] _chunk = null;
					if (executionReq.chunks.size() > 0) {
						synchronized (executionReq.chunks) {
							_chunk = executionReq.chunks.get(0);
							executionReq.chunks.remove(0);
						}
					}
					if (_chunk != null) {
						buffer += new String(_chunk);
						if (!contentStarted) {
							int pos = buffer.indexOf("<uncs:Group ");
							if (pos != -1) {
								buffer = buffer.substring(pos);
								contentStarted = true;
								fos.write(("RPT\ttime\tCURRENCY\tREPORTED_CLASSIFICATION\tTF\tREPORTED_CURRENCY\tCC\tPRT\tnetweight\tqty\tQU\tvalue\tEST\tHT\n").getBytes());
							}
						}
						if (!contentEnded) {
							int pos = buffer.indexOf("</uncs:Group>");
							if (pos != -1) {
								buffer = buffer.substring(0, pos + "</uncs:Group>".length());
								contentEnded = true;
							}
						}
						if (contentStarted) {
							boolean doAgain = true;
							while (doAgain) {
								if (buffer.startsWith("</uncs:Group>")) {
									buffer = buffer.substring("</uncs:Group>".length());
								}
								if (buffer.startsWith("</uncs:Section>")) {
									buffer = buffer.substring("</uncs:Section>".length());
								}
								if (buffer.startsWith("<uncs:Group ")) {
									int posEnd = buffer.indexOf(">");
									if (posEnd > -1) {
										groupRow = new GroupRow(buffer.substring("<uncs:Group ".length(), posEnd));
										buffer = buffer.substring(posEnd + 1);
									} else {
										doAgain = false;
									}
								} else if (buffer.startsWith("<uncs:Section ")) {
									int posEnd = buffer.indexOf(">");
									if (posEnd > -1) {
										sectRow = new SectRow(buffer.substring("<uncs:Section ".length(), posEnd));
										buffer = buffer.substring(posEnd + 1);
									} else {
										doAgain = false;
									}
								} else if (buffer.startsWith("<uncs:Obs ")) {
									int posEnd = buffer.indexOf(">");
									if (posEnd > -1) {
										ObsRow obsRow = new ObsRow(buffer.substring("<uncs:Obs ".length(), posEnd - 1));
										fos.write((groupRow.toCsv() + "\t" + sectRow.toCsv() + "\t" + obsRow.toCsv() + "\n").getBytes());
										buffer = buffer.substring(posEnd + 1);
									} else {
										doAgain = false;
									}
								} else {
									doAgain = false;
								}
							}
							
						}
						if (++cnt % 5000 == 0) {
							executionReq.log(this.threadNumber, this.cntExecutions, "parser - chunk written " + (cnt));
						}
						
					}
				}
				fos.flush();
				executionReq.log(this.threadNumber, this.cntExecutions, "parser - end detected");
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			try {
				executionReq.log(this.threadNumber, this.cntExecutions, "parser - thread ended");
			} catch (Exception e) {}
		}
	}
}
