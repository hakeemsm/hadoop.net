using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <seealso cref="TestDelegatingInputFormat"/>
	public class TestMultipleInputs : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestMultipleInputs()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		private static readonly Path RootDir = new Path("testing/mo");

		private static readonly Path In1Dir = new Path(RootDir, "input1");

		private static readonly Path In2Dir = new Path(RootDir, "input2");

		private static readonly Path OutDir = new Path(RootDir, "output");

		private Path GetDir(Path dir)
		{
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (IsLocalFS())
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").Replace(' '
					, '+');
				dir = new Path(localPathRoot, dir);
			}
			return dir;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		protected override void SetUp()
		{
			base.SetUp();
			Path rootDir = GetDir(RootDir);
			Path in1Dir = GetDir(In1Dir);
			Path in2Dir = GetDir(In2Dir);
			Configuration conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(rootDir, true);
			if (!fs.Mkdirs(in1Dir))
			{
				throw new IOException("Mkdirs failed to create " + in1Dir.ToString());
			}
			if (!fs.Mkdirs(in2Dir))
			{
				throw new IOException("Mkdirs failed to create " + in2Dir.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDoMultipleInputs()
		{
			Path in1Dir = GetDir(In1Dir);
			Path in2Dir = GetDir(In2Dir);
			Path outDir = GetDir(OutDir);
			Configuration conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(outDir, true);
			DataOutputStream file1 = fs.Create(new Path(in1Dir, "part-0"));
			file1.WriteBytes("a\nb\nc\nd\ne");
			file1.Close();
			// write tab delimited to second file because we're doing
			// KeyValueInputFormat
			DataOutputStream file2 = fs.Create(new Path(in2Dir, "part-0"));
			file2.WriteBytes("a\tblah\nb\tblah\nc\tblah\nd\tblah\ne\tblah");
			file2.Close();
			Job job = Job.GetInstance(conf);
			job.SetJobName("mi");
			MultipleInputs.AddInputPath(job, in1Dir, typeof(TextInputFormat), typeof(TestMultipleInputs.MapClass
				));
			MultipleInputs.AddInputPath(job, in2Dir, typeof(KeyValueTextInputFormat), typeof(
				TestMultipleInputs.KeyValueMapClass));
			job.SetMapOutputKeyClass(typeof(Text));
			job.SetMapOutputValueClass(typeof(Text));
			job.SetOutputKeyClass(typeof(NullWritable));
			job.SetOutputValueClass(typeof(Text));
			job.SetReducerClass(typeof(TestMultipleInputs.ReducerClass));
			FileOutputFormat.SetOutputPath(job, outDir);
			bool success = false;
			try
			{
				success = job.WaitForCompletion(true);
			}
			catch (Exception ie)
			{
				throw new RuntimeException(ie);
			}
			catch (TypeLoadException instante)
			{
				throw new RuntimeException(instante);
			}
			if (!success)
			{
				throw new RuntimeException("Job failed!");
			}
			// copy bytes a bunch of times for the ease of readLine() - whatever
			BufferedReader output = new BufferedReader(new InputStreamReader(fs.Open(new Path
				(outDir, "part-r-00000"))));
			// reducer should have counted one key from each file
			NUnit.Framework.Assert.IsTrue(output.ReadLine().Equals("a 2"));
			NUnit.Framework.Assert.IsTrue(output.ReadLine().Equals("b 2"));
			NUnit.Framework.Assert.IsTrue(output.ReadLine().Equals("c 2"));
			NUnit.Framework.Assert.IsTrue(output.ReadLine().Equals("d 2"));
			NUnit.Framework.Assert.IsTrue(output.ReadLine().Equals("e 2"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAddInputPathWithFormat()
		{
			Job conf = Job.GetInstance();
			MultipleInputs.AddInputPath(conf, new Path("/foo"), typeof(TextInputFormat));
			MultipleInputs.AddInputPath(conf, new Path("/bar"), typeof(KeyValueTextInputFormat
				));
			IDictionary<Path, InputFormat> inputs = MultipleInputs.GetInputFormatMap(conf);
			NUnit.Framework.Assert.AreEqual(typeof(TextInputFormat), inputs[new Path("/foo")]
				.GetType());
			NUnit.Framework.Assert.AreEqual(typeof(KeyValueTextInputFormat), inputs[new Path(
				"/bar")].GetType());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAddInputPathWithMapper()
		{
			Job conf = Job.GetInstance();
			MultipleInputs.AddInputPath(conf, new Path("/foo"), typeof(TextInputFormat), typeof(
				TestMultipleInputs.MapClass));
			MultipleInputs.AddInputPath(conf, new Path("/bar"), typeof(KeyValueTextInputFormat
				), typeof(TestMultipleInputs.KeyValueMapClass));
			IDictionary<Path, InputFormat> inputs = MultipleInputs.GetInputFormatMap(conf);
			IDictionary<Path, Type> maps = MultipleInputs.GetMapperTypeMap(conf);
			NUnit.Framework.Assert.AreEqual(typeof(TextInputFormat), inputs[new Path("/foo")]
				.GetType());
			NUnit.Framework.Assert.AreEqual(typeof(KeyValueTextInputFormat), inputs[new Path(
				"/bar")].GetType());
			NUnit.Framework.Assert.AreEqual(typeof(TestMultipleInputs.MapClass), maps[new Path
				("/foo")]);
			NUnit.Framework.Assert.AreEqual(typeof(TestMultipleInputs.KeyValueMapClass), maps
				[new Path("/bar")]);
		}

		internal static readonly Text blah = new Text("blah");

		internal class MapClass : Mapper<LongWritable, Text, Text, Text>
		{
			// these 3 classes do a reduce side join with 2 different mappers
			// receives "a", "b", "c" as values
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context ctx)
			{
				ctx.Write(value, blah);
			}
		}

		internal class KeyValueMapClass : Mapper<Text, Text, Text, Text>
		{
			// receives "a", "b", "c" as keys
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(Text key, Text value, Mapper.Context ctx)
			{
				ctx.Write(key, blah);
			}
		}

		internal class ReducerClass : Reducer<Text, Text, NullWritable, Text>
		{
			internal int count = 0;

			// should receive 2 rows for each key
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<Text> values, Reducer.Context
				 ctx)
			{
				count = 0;
				foreach (Text value in values)
				{
					count++;
				}
				ctx.Write(NullWritable.Get(), new Text(key.ToString() + " " + count));
			}
		}
	}
}
