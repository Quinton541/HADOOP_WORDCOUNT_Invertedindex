import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class WordCount {

    public static class TokenizerFileMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one = new IntWritable(1); // map输出的value
        private Text word = new Text(); // map输出的key
        private Set<String> wordsToSkip = new HashSet<String>();   //停词
        private Set<String> punctuations = new HashSet<String>(); // 标点符号
        private Configuration conf;
        private BufferedReader fis; // 文件输入流

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName); // 将停词放入需要跳过的范围
                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName); // 将标点符号放入需要跳过的范围
            }
        }

        private void parseSkipFile(String fileName) {//parseSkip函数处理两个停词文件，分析并加入wordstoSkip与punctuations的集合
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String temp = null;
                while ((temp = fis.readLine()) != null) {
                    wordsToSkip.add(temp);
                }
            } catch (IOException ioe) {
                System.err.println("Error parsing the file " + StringUtils.stringifyException(ioe));
            }
        }
        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String temp= null;
                while ((temp = fis.readLine()) != null) {
                    punctuations.add(temp);
                }
            } catch (IOException ioe) {
                System.err.println("Error parsing the file" + StringUtils.stringifyException(ioe));
            }
        }
        //以上预处理结束
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString().toLowerCase();
            for (String pattern : punctuations) { // 将数据中所有满足patternsToSkip的pattern都过滤掉, replace by ""
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken(); //从迭代器中读取单词
                if(temp.length()>=3&&!Pattern.compile("^[-\\+]?[\\d]*$").matcher(temp).matches()&&!wordsToSkip.contains(temp)) {
                    word.set(temp+"#"+fileName);
                    context.write(word, one);
                    Counter counter = context.getCounter(
                            CountersEnum.class.getName(),
                            CountersEnum.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }
    }
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one = new IntWritable(1); // map输出的value
        private Text word = new Text(); // map输出的key
        private Set<String> wordsToSkip = new HashSet<String>();   //停词
        private Set<String> punctuations = new HashSet<String>(); // 标点符号
        private Configuration conf;
        private BufferedReader fis; // 文件输入流

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName); // 将停词放入需要跳过的范围
                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName); // 将标点符号放入需要跳过的范围
            }
        }
        private void parseSkipFile(String fileName) {//parseSkip函数处理两个停词文件，分析并加入wordstoSkip与punctuations的集合
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String temp = null;
                while ((temp = fis.readLine()) != null) {
                    wordsToSkip.add(temp);
                }
            } catch (IOException ioe) {
                System.err.println("Error parsing the file " + StringUtils.stringifyException(ioe));
            }
        }
        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String temp= null;
                while ((temp = fis.readLine()) != null) {
                    punctuations.add(temp);
                }
            } catch (IOException ioe) {
                System.err.println("Error parsing the file" + StringUtils.stringifyException(ioe));
            }
        }
        //以上预处理结束
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString().toLowerCase();
            for (String pattern : punctuations) { // 将数据中所有满足patternsToSkip的pattern都过滤掉, replace by ""
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken(); //从迭代器中读取单词
                if(temp.length()>=3&&!Pattern.compile("^[-\\+]?[\\d]*$").matcher(temp).matches()&&!wordsToSkip.contains(temp)) {
                    word.set(temp);
                    context.write(word, one);
                    Counter counter = context.getCounter(
                            TokenizerFileMapper.CountersEnum.class.getName(),
                            TokenizerFileMapper.CountersEnum.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }
    }
    public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = new String();
            term = key.toString().split("-")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = new String();
        static Text CurrentWord = new Text(" ");
        static List<String> wordList = new ArrayList<String>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split("-")[0]);
            temp = key.toString().split("-")[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            word2.set(sum + "#" + temp);
            if (!CurrentWord.equals(word1) && !CurrentWord.equals(" ")) {
                Collections.sort(wordList,Collections.reverseOrder());
                StringBuilder out = new StringBuilder();
                int len = wordList.size();
                int i = 0;
                int count = 0;
                for (String p : wordList) {
                    String docId = p.toString().split("-")[1];
                    String wordCount = p.toString().split("-")[0];
                    count += Integer.parseInt(wordCount);
                    out.append(docId + "#" + wordCount);
                    if(i != len-1){
                        out.append(", ");
                        i++;
                    }
                }
                if(count != 0)
                    context.write(new Text(CurrentWord+": "+out.toString()), NullWritable.get());
                wordList = new ArrayList<String>();
            }
            CurrentWord = new Text(word1);
            wordList.add(word2.toString()); 
        }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 5) {
            System.err.println("Error in command parameters");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Inverted Indexer");
        job.setJar("WordCount.jar");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerFileMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; i++) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}