import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class SqlPerformanceTester {

    private static final String CONFIG_FILE = "config.properties";
    private static final String QUERIES_FILE = "queries.sql";
    private static final String FILE_PREFIX = "performance_results_";
    private static final String INPUT_FILE = "fullLogs.log";
    public static int globalOffset = 0;
    public static int globalOffsetLines = 0;
    private static final String START_TEXT = "Request completed in ";
    private static final String END_TEXT = "ms";

    //Class for getting the Timestamp and duration
    public static class HTTPDurationResult {
        private final String timestamp;
        private final long sumOfDurations;

        public HTTPDurationResult(String timestamp, long sumOfDurations) {
            this.timestamp = timestamp;
            this.sumOfDurations = sumOfDurations;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public long getSumOfDurations() {
            return sumOfDurations;
        }
    }

    private static final Logger LOGGER = Logger.getLogger(SqlPerformanceTester.class.getName());

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage 1 (Single Query): java SqlPerformanceTester <query_name_or_index> <iterations>");
            System.out.println("Usage 2 (All Queries): java SqlPerformanceTester all <iterations>");
            System.out.println("Example (Single): java SqlPerformanceTester Q2 100");
            System.out.println("Example (All): java SqlPerformanceTester all 50");
            return;
        }
        
        // --- 0. Setup File Names ---
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date().getTime());
        String csvFileName = FILE_PREFIX + timestamp + ".csv";
        String logFileName = FILE_PREFIX + timestamp + ".log";
        
        setupLogging(logFileName);

        String targetQueryIdentifier = args[0];
        int iterations;
        try {
            iterations = Integer.parseInt(args[1]);
            if (iterations <= 0) {
                 LOGGER.severe("Error: Iterations must be a positive integer.");
                 System.err.println("Error: Iterations must be a positive integer.");
                 return;
            }
        } catch (NumberFormatException e) {
            LOGGER.severe("Error: The second argument (iterations) must be an integer.");
            System.err.println("Error: The second argument (iterations) must be an integer.");
            return;
        }

        LOGGER.config("Starting SQL Performance Tester with arguments: " + Arrays.toString(args));

        // --- 1. Load Configuration and Queries ---
        Properties props = loadProperties();
        if (props == null) return;

        Map<String, String> allQueries = loadQueries();
        if (allQueries == null || allQueries.isEmpty()) {
            LOGGER.severe("Error: Could not load queries from " + QUERIES_FILE + ".");
            System.err.println("Error: Could not load queries from " + QUERIES_FILE + ".");
            return;
        }

        // --- 2. Determine Target Queries ---
        Map<String, String> queriesToRun = new LinkedHashMap<>();
        boolean runAll = targetQueryIdentifier.equalsIgnoreCase("all");
        
        if (runAll) {
            queriesToRun.putAll(allQueries);
            System.out.printf("--- Performance Test Setup ---\n");
            System.out.printf("Target: ALL %d queries\n", allQueries.size());
        } else {
            // Logic for a single query (existing behavior)
            String queryToExecute = null;
            String queryName = null;

            // Try to resolve by name (e.g., "Q1")
            if (allQueries.containsKey(targetQueryIdentifier)) {
                queryName = targetQueryIdentifier;
                queryToExecute = allQueries.get(queryName);
            } else {
                // Try to resolve by index (e.g., "1" for the 1st query)
                try {
                    int index = Integer.parseInt(targetQueryIdentifier);
                    List<String> queryKeys = new ArrayList<>(allQueries.keySet());
                    if (index > 0 && index <= queryKeys.size()) {
                        queryName = queryKeys.get(index - 1);
                        queryToExecute = allQueries.get(queryName);
                    }
                } catch (NumberFormatException ignored) {}
            }

            if (queryToExecute == null) {
                LOGGER.warning("Query identifier '" + targetQueryIdentifier + "' not found.");
                System.err.println("Error: Query identifier '" + targetQueryIdentifier + "' not found.");
                System.err.println("Available queries: " + allQueries.keySet());
                return;
            }
            queriesToRun.put(queryName, queryToExecute);
            System.out.printf("--- Performance Test Setup ---\n");
            System.out.printf("Target: Single Query (%s)\n", queryName);
        }

        System.out.printf("Iterations Per Query: %d\n", iterations);
        System.out.printf("Total Executions: %d\n", queriesToRun.size() * iterations);
        System.out.printf("Output CSV: %s\n", csvFileName);
        System.out.printf("Output Log: %s\n\n", logFileName);
        LOGGER.info("CSV output path: " + csvFileName);
        LOGGER.info("Log output path: " + logFileName);

        // --- 3. Execute Test(s) ---
        runPerformanceTests(props, queriesToRun, iterations, csvFileName);
    }
    private static void setupLogging(String logFileName) {
        LOGGER.setLevel(Level.FINER); // Set logger level to FINE (Verbosity 2)
        
        // Remove existing handlers to prevent duplicate output
        for (Handler handler : LOGGER.getHandlers()) {
            LOGGER.removeHandler(handler);
        }

        try {
            // File Handler: Logs detailed information to the file
            FileHandler fileHandler = new FileHandler(logFileName, true); // Append mode
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.FINER);
            LOGGER.addHandler(fileHandler);

            // Console Handler: Logs INFO and above to the console (user-facing status)
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.FINER);
            LOGGER.addHandler(consoleHandler);

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not set up file logging.", e);
        }
    }

    /**
     * Loads JDBC connection properties from config.properties.
     */
    private static Properties loadProperties() {
        Properties props = new Properties();
        LOGGER.info("Loading configuration from " + CONFIG_FILE + "...");
        try (InputStream input = Files.newInputStream(Paths.get(CONFIG_FILE))) {
            props.load(input);
            return props;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Error: Could not read " + CONFIG_FILE, ex);
            System.err.println("Error: Could not read " + CONFIG_FILE + ". Ensure it exists and is correctly formatted.");
            return null;
        }
    }

    /**
     * Loads SQL queries from queries.sql. 
     * Uses $$QueryName$$ delimiters.
     */
    private static Map<String, String> loadQueries() {
        Map<String, String> queries = new LinkedHashMap<>();
        try {
            String content = new String(Files.readAllBytes(Paths.get(QUERIES_FILE)));
            
            // Pattern to find $$QueryName$$ markers across multiple lines
            // (\\s*\\$\\$\\s*([^$]*?)\\s*\\$\\$\\s*) captures the full marker block
            Pattern pattern = Pattern.compile("(\\s*\\$\\$\\s*([^$]*?)\\s*\\$\\$\\s*)", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(content);
            
            int lastEnd = 0;
            String previousName = null;
            LOGGER.fine("Starting query parsing...");

            while (matcher.find()) {
                if (previousName != null) {
                    // Extract the query content between the previous marker and the current one
                    String queryContent = content.substring(lastEnd, matcher.start()).trim();
                    if (!queryContent.isEmpty()) {
                        queries.put(previousName, queryContent);
                        LOGGER.fine("Parsed Query: " + previousName + " (Length: " + queryContent.length() + ")");
                    }
                }
                
                // Group 2 is the actual query name between $$ markers
                previousName = matcher.group(2).trim();
                lastEnd = matcher.end();
            }

            // Handle the final query block (from the last marker to the end of the file)
            if (previousName != null) {
                String finalQueryContent = content.substring(lastEnd).trim();
                if (!finalQueryContent.isEmpty()) {
                    queries.put(previousName, finalQueryContent);
                    LOGGER.fine("Parsed Final Query: " + previousName + " (Length: " + finalQueryContent.length() + ")");
                }
            }
            LOGGER.info("Successfully parsed " + queries.size() + " queries.");

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error reading queries file: " + QUERIES_FILE, e);
            return null;
        }
        return queries;
    }

    public static HTTPDurationResult extractHTTPDurationVerbosity2Timestamp(String sourcePath, int offset) {
        
        // --- Step 1: Initialization ---
        
        List<Long> extractedNumbers = new ArrayList<>();
        String extractedTimestamp = null; 
        
        int startIndex = START_TEXT.length(); 
        int currentLineNumber = 0; // Tracks the physical line number in the file (0-indexed)
        
        // --- Step 2: Read the file, extract numbers, and the specific timestamp ---
        
        try (BufferedReader br = new BufferedReader(new FileReader(sourcePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                
                // A. Check for Timestamp Extraction
                if (currentLineNumber == SqlPerformanceTester.globalOffsetLines) {
                    int tsEnd = line.indexOf("\t");
                    if (tsEnd != -1) {
                        // Extract the timestamp up to the first decimal point
                        extractedTimestamp = line.substring(0, tsEnd); 
                    }
                }
                
                // B. Check for Duration Extraction
                int start = line.indexOf(START_TEXT);
                if (start != -1) {
                    int numberStart = start + startIndex;
                    int numberEnd = line.indexOf(END_TEXT, numberStart);

                    if (numberEnd != -1) {
                        try {
                            String numberStr = line.substring(numberStart, numberEnd).trim();
                            extractedNumbers.add(Long.parseLong(numberStr));
                        } catch (NumberFormatException e) {
                            // Skip lines where number extraction fails
                        }
                    }
                }
                
                currentLineNumber++;
            }
        } catch (IOException e) {
            System.err.println("Error reading source file: " + sourcePath);
            e.printStackTrace();
            return new HTTPDurationResult(null, 0); 
        }

        int initialListSizeE = extractedNumbers.size();
        
        
        if (offset > 0 && offset < initialListSizeE) {
            extractedNumbers = extractedNumbers.subList(offset, initialListSizeE);
        } else if (offset >= initialListSizeE) {
            extractedNumbers.clear();
        }
        
        long totalSum = 0;
        for (Long number : extractedNumbers) {
            totalSum += number;
        }

        SqlPerformanceTester.globalOffset = initialListSizeE;
        SqlPerformanceTester.globalOffsetLines = currentLineNumber;
        
        return new HTTPDurationResult(extractedTimestamp, totalSum);
    }

    /**
     * Connects to the database and runs the performance test for all specified queries.
     */
    private static void runPerformanceTests(Properties props, Map<String, String> queriesToRun, int iterations, String csvFileName) {
        String url = props.getProperty("jdbc.url");
        String user = props.getProperty("jdbc.username");
        String password = props.getProperty("jdbc.password");
        String driverClass = props.getProperty("jdbc.driver.class");

        if (url == null || user == null || password == null || driverClass == null) {
            LOGGER.severe("Missing required properties in config.properties.");
            System.err.println("Missing required properties in config.properties.");
            return;
        }

        // 0 Clean up Env
        try {
            File logs = new File(INPUT_FILE);
            if (logs.delete()) {
                System.out.println("deleted the file");
            }
        } catch (Error e){
        }


        // --- 1. Load JDBC Driver ---
        try {
            LOGGER.info("Attempting to load JDBC driver: " + driverClass);
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "FATAL ERROR: JDBC Driver class '" + driverClass + "' not found. Check classpath.", e);
            System.err.println("FATAL ERROR: JDBC Driver class '" + driverClass + "' not found. Check classpath.");
            return;
        }

        // --- 2. Run Tests ---
        List<String> allSummaries = new ArrayList<>(); 
        
        try (
            // Establish connection before the loop
            Connection conn = DriverManager.getConnection(url, user, password);
            PrintWriter csvWriter = new PrintWriter(new FileWriter(csvFileName, false))
        ) {
            LOGGER.info("Connection established successfully to: " + url);
            System.out.println("Connection established successfully.");
            
            // CSV Header
            csvWriter.println("Query Name,Iteration,start-res,res-read,total,HTTPLogTime,start-HTTPStart");

            for (Map.Entry<String, String> entry : queriesToRun.entrySet()) {
                String queryName = entry.getKey();
                String query = entry.getValue();
                
                System.out.println("\n--- Starting Test for Query: " + queryName + " ---");
                LOGGER.info("Starting run for query: " + queryName);
                
                List<Long> durations = new ArrayList<>();
                long totalDurationNanos = 0;

                try (PreparedStatement stmt = conn.prepareStatement(query)) {
                    LOGGER.fine("Prepared statement for query: " + queryName);
                    
                    for (int i = 1; i <= iterations; i++) {
                        System.out.printf("  Executing (Run %d/%d)... ", i, iterations);
                        LOGGER.fine("Executing run " + i + " of " + iterations);
                        
                        long startTimeNanos = System.nanoTime();
                        Instant startTimeInstant = Instant.now();
                        long resultReturnTimeNanos = System.nanoTime();
                        try (ResultSet rs = stmt.executeQuery()) {
                            // Consume the entire result set
                            
                            resultReturnTimeNanos = System.nanoTime();
                            
                            //long preProcessing =  TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                            int rowCount = 0;
                            Object colValue;
                            while (rs.next()) { 
                                rowCount++;
                                for (int l=1; l< rs.getMetaData().getColumnCount(); l++) {
                                    colValue = rs.getObject(l);
                                    //System.out.println(rs.getObject(l));
                                }
                            }
                            LOGGER.fine("Run " + i + " returned " + rowCount + " rows.");
                        } catch (SQLException e) {
                            LOGGER.log(Level.WARNING, "SQL Execution Error in Run " + i + " for query " + queryName, e);
                            System.err.printf("\n  --- SQL ERROR in Run %d ---\n", i);
                            System.err.println("  Message: " + e.getMessage());
                            System.err.println("  ---------------------------\n");
                            continue; // Skip timing for this failed run
                        }
                        
                        long resultParsedAllTimeNanos = System.nanoTime();


                        long durationNanos = resultParsedAllTimeNanos - startTimeNanos;
                        long durationMs = TimeUnit.NANOSECONDS.toMillis(resultParsedAllTimeNanos - startTimeNanos);
                         

                        durations.add(durationMs);
                        totalDurationNanos += durationNanos;


                        System.out.printf("Time: %d ms\n", durationMs);
                        LOGGER.fine("Run " + i + " duration: " + durationMs + " ms");

                        // Write to CSV
                        //String sanitizedQuery = query.replace('\n', ' ');

                        HTTPDurationResult runHTTPResult = extractHTTPDurationVerbosity2Timestamp(INPUT_FILE,SqlPerformanceTester.globalOffset);
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                        long durationPPMS = 0;
                        //OffsetDateTime odt;

                        OffsetDateTime odt = OffsetDateTime.parse(runHTTPResult.getTimestamp().replace("'", ""), formatter);
                        Instant HTTPInstant = odt.toInstant();
                        Duration PreProcessing = Duration.between(startTimeInstant, HTTPInstant);
                        durationPPMS = PreProcessing.toMillis();

                        csvWriter.printf("%s,%d,%d,%d,%d,%d,%d\n", 
                            queryName, 
                            i, 
                            TimeUnit.NANOSECONDS.toMillis(resultReturnTimeNanos - startTimeNanos),
                            TimeUnit.NANOSECONDS.toMillis(resultParsedAllTimeNanos - resultReturnTimeNanos),
                            TimeUnit.NANOSECONDS.toMillis(resultParsedAllTimeNanos - startTimeNanos), 
                            //startTimeNanos,
                            //extractHTTPDurationVerbosity2(INPUT_FILE,SqlPerformanceTester.globalOffset)
                            runHTTPResult.getSumOfDurations(),
                            //runHTTPResult.getTimestamp(),
                            //startTimeNanos,
                            durationPPMS //runHTTPResult.getTimestamp(),
                           
                        );

                    }
                    
                    // Generate and store summary for this query
                    if (!durations.isEmpty()) {
                        double averageDurationMs = (double) totalDurationNanos / iterations / 1_000_000.0;
                        long minDurationMs = durations.stream().mapToLong(l -> l).min().orElse(0);
                        long maxDurationMs = durations.stream().mapToLong(l -> l).max().orElse(0);
                        
                        String summary = String.format("Query %s: Avg=%.2f ms, Min=%d ms, Max=%d ms", 
                            queryName, averageDurationMs, minDurationMs, maxDurationMs);
                        allSummaries.add(summary);
                        LOGGER.info("Summary for " + queryName + ": " + summary);
                    }
                } catch (SQLException e) {
                    LOGGER.log(Level.SEVERE, "Fatal Error Preparing Query " + queryName, e);
                    System.err.println("\n--- Fatal Error Preparing Query " + queryName + " ---");
                    e.printStackTrace();
                }

            }
        
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "FATAL SQL ERROR: Could not execute test or establish connection.", e);
            System.err.println("\n--- FATAL SQL ERROR ---\nCould not execute test or establish connection.");
            e.printStackTrace();
            return;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "FATAL FILE ERROR: Could not write to CSV file.", e);
            System.err.println("\n--- FATAL FILE ERROR ---\nCould not write to CSV file.");
            e.printStackTrace();
            return;
        }

        // --- 3. Display Final Summary ---
        System.out.println("\n==================================");
        System.out.println("--- ALL TESTS COMPLETED SUMMARY ---");
        System.out.println("Results exported to " + csvFileName);
        //System.out.println("Log details exported to " + logFileName);
        System.out.println("Total queries run: " + queriesToRun.size());
        System.out.println("Iterations per query: " + iterations);
        System.out.println("----------------------------------");
        allSummaries.forEach(System.out::println);
        System.out.println("==================================\n");
    }
}


