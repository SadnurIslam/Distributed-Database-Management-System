import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount {

    // ============================================================
    // MAPPER
    // ============================================================

    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String line = value.toString();

            // Choose ONLY ONE function

            wordFrequency(line, output);
            // characterFrequency(line, output);
            // wordLengthFrequency(line, output);
            // lineLengthCount(line, output);
            // wordsPerLine(line, key, output);
            // charactersPerLine(line, key, output);
        }
    }


    // ============================================================
    // 1. WORD FREQUENCY
    // ============================================================

    public static void wordFrequency(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {

            String word = tokenizer.nextToken();

            output.collect(
                    new Text(word),
                    new IntWritable(1)
            );
        }
    }


    // ============================================================
    // 2. CHARACTER FREQUENCY
    // ============================================================

    public static void characterFrequency(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        for (int i = 0; i < line.length(); i++) {

            char ch = line.charAt(i);

            // Ignore spaces
            if (ch != ' ') {

                output.collect(
                        new Text(String.valueOf(ch)),
                        new IntWritable(1)
                );
            }
        }
    }


    // ============================================================
    // 3. WORD LENGTH FREQUENCY
    // ============================================================

    public static void wordLengthFrequency(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {

            String word = tokenizer.nextToken();

            int length = word.length();

            output.collect(
                    new Text(String.valueOf(length)),
                    new IntWritable(1)
            );
        }
    }


    // ============================================================
    // 4. LINE LENGTH COUNT
    // Number of lines having the same number of words
    // ============================================================

    public static void lineLengthCount(
            String line,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        int wordCount = 0;

        while (tokenizer.hasMoreTokens()) {

            tokenizer.nextToken();
            wordCount++;
        }

        output.collect(
                new Text(String.valueOf(wordCount)),
                new IntWritable(1)
        );
    }


    // ============================================================
    // 5. NUMBER OF WORDS PER LINE
    // ============================================================

    public static void wordsPerLine(
            String line,
            LongWritable key,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        StringTokenizer tokenizer =
                new StringTokenizer(line);

        int wordCount = 0;

        while (tokenizer.hasMoreTokens()) {

            tokenizer.nextToken();
            wordCount++;
        }

        /*
         * key = byte offset of the line.
         * It identifies the input record, but it is NOT
         * necessarily the line number.
         */

        output.collect(
                new Text("Line_" + key.get()),
                new IntWritable(wordCount)
        );
    }


    // ============================================================
    // 6. NUMBER OF CHARACTERS PER LINE
    // ============================================================

    public static void charactersPerLine(
            String line,
            LongWritable key,
            OutputCollector<Text, IntWritable> output)
            throws IOException {

        int characterCount = line.length();

        output.collect(
                new Text("Line_" + key.get()),
                new IntWritable(characterCount)
        );
    }


    // ============================================================
    // REDUCER
    // ============================================================

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(
                Text key,
                Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output,
                Reporter reporter)
                throws IOException {

            int sum = 0;

            while (values.hasNext()) {

                sum += values.next().get();
            }

            output.collect(
                    key,
                    new IntWritable(sum)
            );
        }
    }


    // ============================================================
    // MAIN
    // ============================================================

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(WordCount.class);

        conf.setJobName("MapReduce Task");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(
                conf,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                conf,
                new Path(args[1])
        );

        JobClient.runJob(conf);
    }
}