using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestJavaSerialization : TestCase
	{
		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToURI().ToString().Replace(' ', '+');

		private readonly Path InputDir = new Path(TestRootDir + "/input");

		private readonly Path OutputDir = new Path(TestRootDir + "/out");

		private readonly Path InputFile;

		internal class WordCountMapper : MapReduceBase, Mapper<LongWritable, Text, string
			, long>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<string, long
				> output, Reporter reporter)
			{
				StringTokenizer st = new StringTokenizer(value.ToString());
				while (st.HasMoreTokens())
				{
					string token = st.NextToken();
					NUnit.Framework.Assert.IsTrue("Invalid token; expected 'a' or 'b', got " + token, 
						token.Equals("a") || token.Equals("b"));
					output.Collect(token, 1L);
				}
			}
		}

		internal class SumReducer<K> : MapReduceBase, Reducer<K, long, K, long>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(K key, IEnumerator<long> values, OutputCollector<K, long
				> output, Reporter reporter)
			{
				long sum = 0;
				while (values.HasNext())
				{
					sum += values.Next();
				}
				output.Collect(key, sum);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanAndCreateInput(FileSystem fs)
		{
			fs.Delete(InputFile, true);
			fs.Delete(OutputDir, true);
			OutputStream os = fs.Create(InputFile);
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("b a\n");
			wr.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapReduceJob()
		{
			JobConf conf = new JobConf(typeof(TestJavaSerialization));
			conf.SetJobName("JavaSerialization");
			FileSystem fs = FileSystem.Get(conf);
			CleanAndCreateInput(fs);
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
				 + "org.apache.hadoop.io.serializer.WritableSerialization");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(string));
			conf.SetOutputValueClass(typeof(long));
			conf.SetOutputKeyComparatorClass(typeof(JavaSerializationComparator));
			conf.SetMapperClass(typeof(TestJavaSerialization.WordCountMapper));
			conf.SetReducerClass(typeof(TestJavaSerialization.SumReducer));
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			FileInputFormat.SetInputPaths(conf, InputDir);
			FileOutputFormat.SetOutputPath(conf, OutputDir);
			string inputFileContents = FileUtils.ReadFileToString(new FilePath(InputFile.ToUri
				().GetPath()));
			NUnit.Framework.Assert.IsTrue("Input file contents not as expected; contents are '"
				 + inputFileContents + "', expected \"b a\n\" ", inputFileContents.Equals("b a\n"
				));
			JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(fs.ListStatus(OutputDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			NUnit.Framework.Assert.AreEqual(1, outputFiles.Length);
			InputStream @is = fs.Open(outputFiles[0]);
			string reduceOutput = IOUtils.ToString(@is);
			string[] lines = reduceOutput.Split(Runtime.GetProperty("line.separator"));
			NUnit.Framework.Assert.AreEqual("Unexpected output; received output '" + reduceOutput
				 + "'", "a\t1", lines[0]);
			NUnit.Framework.Assert.AreEqual("Unexpected output; received output '" + reduceOutput
				 + "'", "b\t1", lines[1]);
			NUnit.Framework.Assert.AreEqual("Reduce output has extra lines; output is '" + reduceOutput
				 + "'", 2, lines.Length);
			@is.Close();
		}

		/// <summary>
		/// HADOOP-4466:
		/// This test verifies the JavSerialization impl can write to
		/// SequenceFiles.
		/// </summary>
		/// <remarks>
		/// HADOOP-4466:
		/// This test verifies the JavSerialization impl can write to
		/// SequenceFiles. by virtue other SequenceFileOutputFormat is not
		/// coupled to Writable types, if so, the job will fail.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestWriteToSequencefile()
		{
			JobConf conf = new JobConf(typeof(TestJavaSerialization));
			conf.SetJobName("JavaSerialization");
			FileSystem fs = FileSystem.Get(conf);
			CleanAndCreateInput(fs);
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
				 + "org.apache.hadoop.io.serializer.WritableSerialization");
			conf.SetInputFormat(typeof(TextInputFormat));
			// test we can write to sequence files
			conf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			conf.SetOutputKeyClass(typeof(string));
			conf.SetOutputValueClass(typeof(long));
			conf.SetOutputKeyComparatorClass(typeof(JavaSerializationComparator));
			conf.SetMapperClass(typeof(TestJavaSerialization.WordCountMapper));
			conf.SetReducerClass(typeof(TestJavaSerialization.SumReducer));
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			FileInputFormat.SetInputPaths(conf, InputDir);
			FileOutputFormat.SetOutputPath(conf, OutputDir);
			JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(fs.ListStatus(OutputDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			NUnit.Framework.Assert.AreEqual(1, outputFiles.Length);
		}

		public TestJavaSerialization()
		{
			InputFile = new Path(InputDir, "inp");
		}
	}
}
