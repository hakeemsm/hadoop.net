using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Chain
{
	public class TestMapReduceChain : HadoopTestCase
	{
		private static string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp"
			);

		private static Path flagDir = new Path(localPathRoot, "testing/chain/flags");

		/// <exception cref="System.IO.IOException"/>
		private static void CleanFlags(Configuration conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(flagDir, true);
			fs.Mkdirs(flagDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFlag(Configuration conf, string flag)
		{
			FileSystem fs = FileSystem.Get(conf);
			if (GetFlag(conf, flag))
			{
				Fail("Flag " + flag + " already exists");
			}
			DataOutputStream file = fs.Create(new Path(flagDir, flag));
			file.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static bool GetFlag(Configuration conf, string flag)
		{
			FileSystem fs = FileSystem.Get(conf);
			return fs.Exists(new Path(flagDir, flag));
		}

		/// <exception cref="System.IO.IOException"/>
		public TestMapReduceChain()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestChain()
		{
			Path inDir = new Path(localPathRoot, "testing/chain/input");
			Path outDir = new Path(localPathRoot, "testing/chain/output");
			string input = "1\n2\n";
			string expectedOutput = "0\t1ABCRDEF\n2\t2ABCRDEF\n";
			Configuration conf = CreateJobConf();
			CleanFlags(conf);
			conf.Set("a", "X");
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1, input);
			job.SetJobName("chain");
			Configuration mapAConf = new Configuration(false);
			mapAConf.Set("a", "A");
			ChainMapper.AddMapper(job, typeof(TestMapReduceChain.AMap), typeof(LongWritable), 
				typeof(Text), typeof(LongWritable), typeof(Text), mapAConf);
			ChainMapper.AddMapper(job, typeof(TestMapReduceChain.BMap), typeof(LongWritable), 
				typeof(Text), typeof(LongWritable), typeof(Text), null);
			ChainMapper.AddMapper(job, typeof(TestMapReduceChain.CMap), typeof(LongWritable), 
				typeof(Text), typeof(LongWritable), typeof(Text), null);
			Configuration reduceConf = new Configuration(false);
			reduceConf.Set("a", "C");
			ChainReducer.SetReducer(job, typeof(TestMapReduceChain.RReduce), typeof(LongWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), reduceConf);
			ChainReducer.AddMapper(job, typeof(TestMapReduceChain.DMap), typeof(LongWritable)
				, typeof(Text), typeof(LongWritable), typeof(Text), null);
			Configuration mapEConf = new Configuration(false);
			mapEConf.Set("a", "E");
			ChainReducer.AddMapper(job, typeof(TestMapReduceChain.EMap), typeof(LongWritable)
				, typeof(Text), typeof(LongWritable), typeof(Text), mapEConf);
			ChainReducer.AddMapper(job, typeof(TestMapReduceChain.FMap), typeof(LongWritable)
				, typeof(Text), typeof(LongWritable), typeof(Text), null);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			string str = "flag not set";
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.setup.A"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.setup.B"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.setup.C"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "reduce.setup.R"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.setup.D"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.setup.E"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.setup.F"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.A.value.1"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.A.value.2"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.B.value.1A"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.B.value.2A"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.C.value.1AB"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.C.value.2AB"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "reduce.R.value.1ABC"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "reduce.R.value.2ABC"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.D.value.1ABCR"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.D.value.2ABCR"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.E.value.1ABCRD"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.E.value.2ABCRD"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.F.value.1ABCRDE"));
			NUnit.Framework.Assert.IsTrue(str, GetFlag(conf, "map.F.value.2ABCRDE"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.cleanup.A"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.cleanup.B"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.cleanup.C"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "reduce.cleanup.R"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.cleanup.D"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.cleanup.E"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.cleanup.F"));
			NUnit.Framework.Assert.AreEqual("Outputs doesn't match", expectedOutput, MapReduceTestUtil
				.ReadOutput(outDir, conf));
		}

		public class AMap : TestMapReduceChain.IDMap
		{
			public AMap()
				: base("A", "A")
			{
			}
		}

		public class BMap : TestMapReduceChain.IDMap
		{
			public BMap()
				: base("B", "X")
			{
			}
		}

		public class CMap : TestMapReduceChain.IDMap
		{
			public CMap()
				: base("C", "X")
			{
			}
		}

		public class RReduce : TestMapReduceChain.IDReduce
		{
			public RReduce()
				: base("R", "C")
			{
			}
		}

		public class DMap : TestMapReduceChain.IDMap
		{
			public DMap()
				: base("D", "X")
			{
			}
		}

		public class EMap : TestMapReduceChain.IDMap
		{
			public EMap()
				: base("E", "E")
			{
			}
		}

		public class FMap : TestMapReduceChain.IDMap
		{
			public FMap()
				: base("F", "X")
			{
			}
		}

		public class IDMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			private string name;

			private string prop;

			public IDMap(string name, string prop)
			{
				this.name = name;
				this.prop = prop;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				NUnit.Framework.Assert.AreEqual(prop, conf.Get("a"));
				WriteFlag(conf, "map.setup." + name);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				WriteFlag(context.GetConfiguration(), "map." + name + ".value." + value);
				context.Write(key, new Text(value + name));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				WriteFlag(context.GetConfiguration(), "map.cleanup." + name);
			}
		}

		public class IDReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			private string name;

			private string prop;

			public IDReduce(string name, string prop)
			{
				this.name = name;
				this.prop = prop;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Reducer.Context context)
			{
				Configuration conf = context.GetConfiguration();
				NUnit.Framework.Assert.AreEqual(prop, conf.Get("a"));
				WriteFlag(conf, "reduce.setup." + name);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> values, Reducer.Context
				 context)
			{
				foreach (Text value in values)
				{
					WriteFlag(context.GetConfiguration(), "reduce." + name + ".value." + value);
					context.Write(key, new Text(value + name));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				WriteFlag(context.GetConfiguration(), "reduce.cleanup." + name);
			}
		}
	}
}
