using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestChainMapReduce : HadoopTestCase
	{
		private static Path GetFlagDir(bool local)
		{
			Path flagDir = new Path("testing/chain/flags");
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (local)
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").Replace(' '
					, '+');
				flagDir = new Path(localPathRoot, flagDir);
			}
			return flagDir;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CleanFlags(JobConf conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(GetFlagDir(conf.GetBoolean("localFS", true)), true);
			fs.Mkdirs(GetFlagDir(conf.GetBoolean("localFS", true)));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFlag(JobConf conf, string flag)
		{
			FileSystem fs = FileSystem.Get(conf);
			if (GetFlag(conf, flag))
			{
				Fail("Flag " + flag + " already exists");
			}
			DataOutputStream file = fs.Create(new Path(GetFlagDir(conf.GetBoolean("localFS", 
				true)), flag));
			file.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static bool GetFlag(JobConf conf, string flag)
		{
			FileSystem fs = FileSystem.Get(conf);
			return fs.Exists(new Path(GetFlagDir(conf.GetBoolean("localFS", true)), flag));
		}

		/// <exception cref="System.IO.IOException"/>
		public TestChainMapReduce()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestChain()
		{
			Path inDir = new Path("testing/chain/input");
			Path outDir = new Path("testing/chain/output");
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (IsLocalFS())
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").Replace(' '
					, '+');
				inDir = new Path(localPathRoot, inDir);
				outDir = new Path(localPathRoot, outDir);
			}
			JobConf conf = CreateJobConf();
			conf.SetBoolean("localFS", IsLocalFS());
			conf.SetInt("mapreduce.job.maps", 1);
			CleanFlags(conf);
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
			file.WriteBytes("1\n2\n");
			file.Close();
			conf.SetJobName("chain");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.Set("a", "X");
			JobConf mapAConf = new JobConf(false);
			mapAConf.Set("a", "A");
			ChainMapper.AddMapper(conf, typeof(TestChainMapReduce.AMap), typeof(LongWritable)
				, typeof(Text), typeof(LongWritable), typeof(Text), true, mapAConf);
			ChainMapper.AddMapper(conf, typeof(TestChainMapReduce.BMap), typeof(LongWritable)
				, typeof(Text), typeof(LongWritable), typeof(Text), false, null);
			JobConf reduceConf = new JobConf(false);
			reduceConf.Set("a", "C");
			ChainReducer.SetReducer(conf, typeof(TestChainMapReduce.CReduce), typeof(LongWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), true, reduceConf);
			ChainReducer.AddMapper(conf, typeof(TestChainMapReduce.DMap), typeof(LongWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), false, null);
			JobConf mapEConf = new JobConf(false);
			mapEConf.Set("a", "E");
			ChainReducer.AddMapper(conf, typeof(TestChainMapReduce.EMap), typeof(LongWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), true, mapEConf);
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobClient jc = new JobClient(conf);
			RunningJob job = jc.SubmitJob(conf);
			while (!job.IsComplete())
			{
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "configure.A"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "configure.B"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "configure.C"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "configure.D"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "configure.E"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.A.value.1"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.A.value.2"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.B.value.1"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.B.value.2"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "reduce.C.value.2"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "reduce.C.value.1"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.D.value.1"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.D.value.2"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.E.value.1"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "map.E.value.2"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "close.A"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "close.B"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "close.C"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "close.D"));
			NUnit.Framework.Assert.IsTrue(GetFlag(conf, "close.E"));
		}

		public class AMap : TestChainMapReduce.IDMap
		{
			public AMap()
				: base("A", "A", true)
			{
			}
		}

		public class BMap : TestChainMapReduce.IDMap
		{
			public BMap()
				: base("B", "X", false)
			{
			}
		}

		public class CReduce : TestChainMapReduce.IDReduce
		{
			public CReduce()
				: base("C", "C")
			{
			}
		}

		public class DMap : TestChainMapReduce.IDMap
		{
			public DMap()
				: base("D", "X", false)
			{
			}
		}

		public class EMap : TestChainMapReduce.IDMap
		{
			public EMap()
				: base("E", "E", true)
			{
			}
		}

		public class IDMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			private JobConf conf;

			private string name;

			private string prop;

			private bool byValue;

			public IDMap(string name, string prop, bool byValue)
			{
				this.name = name;
				this.prop = prop;
				this.byValue = byValue;
			}

			public virtual void Configure(JobConf conf)
			{
				this.conf = conf;
				NUnit.Framework.Assert.AreEqual(prop, conf.Get("a"));
				try
				{
					WriteFlag(conf, "configure." + name);
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<LongWritable
				, Text> output, Reporter reporter)
			{
				WriteFlag(conf, "map." + name + ".value." + value);
				key.Set(10);
				output.Collect(key, value);
				if (byValue)
				{
					NUnit.Framework.Assert.AreEqual(10, key.Get());
				}
				else
				{
					NUnit.Framework.Assert.AreNotSame(10, key.Get());
				}
				key.Set(11);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				try
				{
					WriteFlag(conf, "close." + name);
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}
		}

		public class IDReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			private JobConf conf;

			private string name;

			private string prop;

			private bool byValue = false;

			public IDReduce(string name, string prop)
			{
				this.name = name;
				this.prop = prop;
			}

			public virtual void Configure(JobConf conf)
			{
				this.conf = conf;
				NUnit.Framework.Assert.AreEqual(prop, conf.Get("a"));
				try
				{
					WriteFlag(conf, "configure." + name);
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(LongWritable key, IEnumerator<Text> values, OutputCollector
				<LongWritable, Text> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					Text value = values.Next();
					WriteFlag(conf, "reduce." + name + ".value." + value);
					key.Set(10);
					output.Collect(key, value);
					if (byValue)
					{
						NUnit.Framework.Assert.AreEqual(10, key.Get());
					}
					else
					{
						NUnit.Framework.Assert.AreNotSame(10, key.Get());
					}
					key.Set(11);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				try
				{
					WriteFlag(conf, "close." + name);
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}
		}
	}
}
