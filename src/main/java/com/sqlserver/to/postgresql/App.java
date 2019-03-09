package com.sqlserver.to.postgresql;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Predicates.not;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException {
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
            System.out.println( "Unexpected exception:" + exp.getMessage() );
        }
    }

    private static void generateFile(File in, File parentDir, List<String> booleanFields) throws IOException {
        System.out.println(String.format("processing file %s", in.getName()));
        File out = new File(parentDir, String.format("%s.postgre.sql", in.getName()));
        List<String> errors = new ArrayList<>();
        StringBuffer previous = new StringBuffer();
        try (BufferedReader reader = Files.newBufferedReader(in.toPath(), Charset.forName("UTF-16")); BufferedWriter writer = Files.newBufferedWriter(out.toPath())) {
            reader.lines().forEach(s -> {
                if (s.isEmpty() || s.startsWith("GO") || s.startsWith("USE") || s.startsWith("SET IDENTITY_INSERT")) {
                    return;
                }
                String toProcess = previous.append(s).toString();
                try {
                    writer.write(intToBoolean(booleanFields, replacAllHexWithDate(toProcess
                            .replaceAll("INSERT \\[", "INSERT INTO [")
                            .replaceAll("\\[dbo\\]\\.", "")
                            .replaceAll("\\[|\\]", "")
                            .replaceAll("N[']", "'")
                            .replaceAll("N[']", "'")
                            .replaceAll("DateTime", "TIMESTAMP")))
                            + ";");
                    writer.newLine();
                    previous.delete(0, previous.length());
                } catch (IllegalStateException e) {
                    previous.append("\n");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(String.format("error processing line:\n\t %s", toProcess));
                    errors.add(String.format("error while processing line:\n\t %s", toProcess));
                    try {
                        writer.newLine();
                        writer.write(toProcess);
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            });
            System.out.println(String.format("done processing file %s. saved to %s", in.getName(), out.getName()));
        }
        if (!errors.isEmpty()) {
            errors.stream().forEach(System.out::println);
            throw new IllegalStateException("unable to convert. see above errors.");
        }
    }

    private static String intToBoolean(List<String> booleanFields, String in) {
        String numericHolder = "__n_u_m____e_r_ic_";
        String numeric = "Numeric(19, 0)";
        String decimalHolder = "__d_e_c____i_m_al_";
        String decimal = "Decimal(10, 2)";
        String noNumeric = in.replace(numeric, numericHolder).replace(decimal, decimalHolder);
        String[] strings = noNumeric.split("[ ]VALUES[ ]");
        String fieldPart = strings[0].substring(strings[0].indexOf("("), strings[0].indexOf(")"));
        String valuePart = strings[1].substring(strings[1].indexOf("("), strings[1].lastIndexOf(")"));
        String[] fields = fieldPart.split(",");
        List<String> values = Splitter.onPattern(",(?=(?:[^\']*\'[^\']*\')*[^\']*$)").splitToList(valuePart);
        if (fields.length != values.size()) {
            throw new IllegalStateException("values and fields do not match");
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

    public static String readHexToDateString(String in) {
        BigInteger dateInt = null;
        BigInteger timeInt = null;
        try {
            String noPrefix = in.substring(2);
            String datePart = noPrefix.substring(0, 8);
            String timePart = noPrefix.substring(8);
            dateInt = new BigInteger(datePart, 16);
            timeInt = new BigInteger(timePart, 16);
        } catch (Exception e) {
            System.err.println(String.format("in: %s", in));
            throw e;
        }
        return DateTime.parse("1900-01-01", DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC())
                .plusDays(dateInt.intValue())
                .plusMillis(timeInt.intValue()*10/3).toString("yyyy-MM-dd hh:mm:ssZ");
    }

    public static String replacAllHexWithDate(String str) {
        Pattern p = Pattern.compile("(0x)([A-Z0-9]{16}[ ])", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(str);
        Map<String, String> matches = new HashMap<>();
        while(m.find()) {
            matches.put(m.group().trim(), App.readHexToDateString(m.group().trim()));
        }
        String out = str;
        for (String key: matches.keySet()){
            out = out.replace(key, String.format("'%s'", matches.get(key)));
        }
        return out;
    }
}
