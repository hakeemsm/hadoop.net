using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestLocalModeWithNewApis
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestLocalModeWithNewApis
			));

		internal Configuration conf;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewApis()
		{
			Random r = new Random(Runtime.CurrentTimeMillis());
			Path tmpBaseDir = new Path("/tmp/wc-" + r.Next());
			Path inDir = new Path(tmpBaseDir, "input");
			Path outDir = new Path(tmpBaseDir, "output");
			string input = "The quick brown fox\nhas many silly\nred fox sox\n";
			FileSystem inFs = inDir.GetFileSystem(conf);
			FileSystem outFs = outDir.GetFileSystem(conf);
			outFs.Delete(outDir, true);
			if (!inFs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			{
				DataOutputStream file = inFs.Create(new Path(inDir, "part-0"));
				file.WriteBytes(input);
				file.Close();
			}
			Job job = Job.GetInstance(conf, "word count");
			job.SetJarByClass(typeof(TestLocalModeWithNewApis));
			job.SetMapperClass(typeof(TestLocalModeWithNewApis.TokenizerMapper));
			job.SetCombinerClass(typeof(TestLocalModeWithNewApis.IntSumReducer));
			job.SetReducerClass(typeof(TestLocalModeWithNewApis.IntSumReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(IntWritable));
			FileInputFormat.AddInputPath(job, inDir);
			FileOutputFormat.SetOutputPath(job, outDir);
			NUnit.Framework.Assert.AreEqual(job.WaitForCompletion(true), true);
			string output = ReadOutput(outDir, conf);
			NUnit.Framework.Assert.AreEqual("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" + "quick\t1\nred\t1\nsilly\t1\nsox\t1\n"
				, output);
			outFs.Delete(tmpBaseDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string ReadOutput(Path outDir, Configuration conf)
		{
			FileSystem fs = outDir.GetFileSystem(conf);
			StringBuilder result = new StringBuilder();
			Path[] fileList = FileUtil.Stat2Paths(fs.ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			foreach (Path outputFile in fileList)
			{
				Log.Info("Path" + ": " + outputFile);
				BufferedReader file = new BufferedReader(new InputStreamReader(fs.Open(outputFile
					)));
				string line = file.ReadLine();
				while (line != null)
				{
					result.Append(line);
					result.Append("\n");
					line = file.ReadLine();
				}
				file.Close();
			}
			return result.ToString();
		}

		public class TokenizerMapper : Mapper<object, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			, IntWritable>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Org.Apache.Hadoop.IO.Text word = new Org.Apache.Hadoop.IO.Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Org.Apache.Hadoop.IO.Text value, Mapper.Context
				 context)
			{
				StringTokenizer itr = new StringTokenizer(value.ToString());
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					context.Write(word, one);
				}
			}
		}

		public class IntSumReducer : Reducer<Org.Apache.Hadoop.IO.Text, IntWritable, Org.Apache.Hadoop.IO.Text
			, IntWritable>
		{
			private IntWritable result = new IntWritable();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerable<IntWritable
				> values, Reducer.Context context)
			{
				int sum = 0;
				foreach (IntWritable val in values)
				{
					sum += val.Get();
				}
				result.Set(sum);
				context.Write(key, result);
			}
		}
	}
}
