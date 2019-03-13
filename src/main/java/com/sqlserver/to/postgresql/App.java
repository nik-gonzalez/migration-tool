package com.sqlserver.to.postgresql;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Predicates.not;

/**
 * Hello world!
 *
 */
public class App {

    static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("exiting");
            if (previous.length() > 0) {
                File out = new File(".", "error.log");
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(out))) {
                    writer.write(previous.toString());
                } catch (IOException e) {
                    LOG.error("error writing errors", e);
                }
            }
        }));
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addRequiredOption("i", "input", true, "MS Sql File location");
        options.addOption("e", "encoding", true, "encoding. default: utf-16");
        options.addOption("b", "booleanFields", true, "comma separated names of boolean fields. default: enabled, salary_fund_source, reset_password, imported");

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            Path in = Paths.get(line.getOptionValue("i"));
            List<String> booleanFields = Optional
                    .ofNullable(line.getOptionValue("b", "enabled, salary_fund_source, reset_password, imported"))
                    .filter(Objects::nonNull)
                    .filter(not(String::isEmpty))
                    .map(s -> Arrays.asList(s.split(",")))
                    .orElse(Lists.newArrayList()).stream().map(String::trim).collect(Collectors.toList());

            if (Files.notExists(in)) {
                throw new IllegalArgumentException(String.format("input file '%s' not found", line.getOptionValue("i")));
            }

            File outDir = new File(Files.isDirectory(in)? in.toFile(): in.getParent().toFile(), "out");
            if (!outDir.exists()) {
                outDir.mkdirs();
            }

            if (Files.isDirectory(in)) {
                Arrays.stream(in.toFile().listFiles((dir, name) -> name.endsWith(".sql"))).forEach(file -> {
                    try {
                        generateFile(file, outDir, booleanFields);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                generateFile(in.toFile(), outDir, booleanFields);
            }

        }
        catch( ParseException exp ) {
            LOG.error("Unexpected exception:", exp);
        }
    }

    private static StringBuilder previous = new StringBuilder();

    private static void generateFile(File in, File parentDir, List<String> booleanFields) throws IOException {
        LOG.info("processing file {}", in.getName());
        File out = new File(parentDir, String.format("%s.postgre.sql", in.getName()));
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);


//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(in), "UTF-16")); BufferedWriter writer = new BufferedWriter(new FileWriter(out)); BufferedWriter writerTooLong = new BufferedWriter(new FileWriter(outTooLong))) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(in, "r");BufferedWriter writer = new BufferedWriter(new FileWriter(out))) {
            int bytesRead;
            LOG.info("skipping 1st char {}", randomAccessFile.readChar());
            do {
                byte[] bytes = new byte[8192];
                bytesRead = randomAccessFile.read(bytes);
                String[] toProcess = new String(bytes, Charset.forName("UTF-16LE")).split("\n");
                for (int i = 0; i < toProcess.length; i++) {
                    migrateLine(in, booleanFields, previous, count, errorCount, writer, toProcess[i], i == toProcess.length -1);
                }
            } while(bytesRead > -1);

            LOG.info("done processing file {}. processed {} rows with {} errors and saved to {}",
                    in.getName(), count.get(), errorCount.get(), out.getName());
        }
    }

    private static void migrateLine(File in, List<String> booleanFields, StringBuilder previous, AtomicInteger count, AtomicInteger errorCount, BufferedWriter writer, String s, boolean last) {
        LOG.debug("incoming string {}", s);
//
        if (s.isEmpty() || s.equals("GO") ||  s.startsWith("USE [") ||s.startsWith("SET IDENTITY_INSERT")) {
            return;
        }

        if (previous.length() == 0) {
            previous.append("\n");
            LOG.info("appending incoming string {}", s);
        }

        String toProcess = previous.append(s
                .replace("INSERT ", "INSERT INTO ")
                .replaceAll("\\[dbo\\]\\.", "")
                .replaceAll("\\[|\\]", "")
                .replace("N'", "'")
                .replace("AS Numeric(19, 0)", "AS Numeric(19)")
                .replace("DateTime", "TIMESTAMP")
        ).toString();

        try {
            writer.write(intToBoolean(booleanFields, replaceAllHexWithDate(toProcess))
                    .replace("INSERT ", "INSERT INTO ")
                    .replace("dbo.", "")
                    .replaceAll("\\[|\\]", "")
                    .replace("N'", "'")
                    .replace("AS Numeric(19, 0)", "AS Numeric(19)")
                    .replace("DateTime", "TIMESTAMP")+";");
            writer.newLine();
            count.incrementAndGet();
            previous.delete(0, previous.length());
        } catch (MultiLineException e) {
            LOG.debug("{} detected multiline entry. line:\n\t {}", in.getName(), toProcess);
            return;
        } catch (Exception e) {
            errorCount.incrementAndGet();
            LOG.error(String.format("[%s]error processing line:\n\t %s", in.getName(), toProcess), e);
            System.exit(9);
        }
    }

    private static String intToBoolean(List<String> booleanFields, String in) {
        if (!in.contains(" VALUES ")) {
            throw new MultiLineException("incomplete line");
        }
        String numericHolder = "__n_u_m____e_r_ic_";
        String numeric = "Numeric(19, 0)";
        String decimalHolder = "__d_e_c____i_m_al_";
        String decimal = "Decimal(10, 2)";
        String noNumeric = in.replace(numeric, numericHolder).replace(decimal, decimalHolder);
        String[] strings = noNumeric.split(" VALUES ");
        if (strings.length != 2) {
            throw new MultiLineException("not enough tokens");
        }
        String fieldPart = null;
        String valuePart = null;
        try {
            fieldPart = strings[0].substring(strings[0].indexOf("("), strings[0].indexOf(")"));
            valuePart = strings[1].substring(strings[1].indexOf("("), strings[1].lastIndexOf(")"));
        } catch (Exception e) {
            throw new MultiLineException("missing tokens");
        }
        String[] fields = fieldPart.split(",");
        List<String> values = Splitter.onPattern(",(?=(?:[^\']*\'[^\']*\')*[^\']*$)").splitToList(valuePart);
        if (fields.length != values.size()) {
            throw new MultiLineException("values and fields do not match");
        }
        List<String> valueOut = Lists.newArrayList();
        for (int i = 0; i < values.size(); i++) {
            if (booleanFields.contains(fields[i].trim())) {
                valueOut.add(values.get(i).replace("1", "true").replace("0", "false"));
            } else {
                valueOut.add(values.get(i).replace(numericHolder, numeric).replace(decimalHolder, decimal));
            }
        }
        return in.substring(0, in.indexOf(") VALUES (")) + String.format(") VALUES %s)", Joiner.on(", ").join(valueOut));
    }

    private static String readHexToDateString(String in) {
        BigInteger dateInt;
        BigInteger timeInt;
        String noPrefix = in.substring(2);
        String datePart = noPrefix.substring(0, 8);
        String timePart = noPrefix.substring(8);
        dateInt = new BigInteger(datePart, 16);
        timeInt = new BigInteger(timePart, 16);
        return DateTime.parse("1900-01-01", DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC())
                .plusDays(dateInt.intValue())
                .plusMillis(timeInt.intValue()*10/3).toString("yyyy-MM-dd hh:mm:ssZ");
    }

    private static String replaceAllHexWithDate(String str) {
        Pattern p = Pattern.compile("(0x)([A-Z0-9]{16}[ ])", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(str);
        Map<String, String> matches = new HashMap<>();
        while(m.find()) {
            matches.put(m.group().trim(), readHexToDateString(m.group().trim()));
        }
        String out = str;
        for (String key: matches.keySet()){
            out = out.replace(key, String.format("'%s'", matches.get(key)));
        }
        return out;
    }

    private static class MultiLineException extends RuntimeException {
        public MultiLineException(String message) {
            super(message);
        }
    }
}
