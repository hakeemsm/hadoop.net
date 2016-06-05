using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	public class TestWrappedRRClassloader : TestCase
	{
		/// <summary>
		/// Tests the class loader set by
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration.SetClassLoader(Sharpen.ClassLoader)
		/// 	"/>
		/// is inherited by any
		/// <see cref="WrappedRecordReader{K, U}"/>
		/// s created by
		/// <see cref="CompositeRecordReader{K, V, X}"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClassLoader()
		{
			Configuration conf = new Configuration();
			TestWrappedRRClassloader.Fake_ClassLoader classLoader = new TestWrappedRRClassloader.Fake_ClassLoader
				();
			conf.SetClassLoader(classLoader);
			NUnit.Framework.Assert.IsTrue(conf.GetClassLoader() is TestWrappedRRClassloader.Fake_ClassLoader
				);
			FileSystem fs = FileSystem.Get(conf);
			Path testdir = new Path(Runtime.GetProperty("test.build.data", "/tmp")).MakeQualified
				(fs);
			Path @base = new Path(testdir, "/empty");
			Path[] src = new Path[] { new Path(@base, "i0"), new Path("i1"), new Path("i2") };
			conf.Set(CompositeInputFormat.JoinExpr, CompositeInputFormat.Compose("outer", typeof(
				TestWrappedRRClassloader.IF_ClassLoaderChecker), src));
			CompositeInputFormat<NullWritable> inputFormat = new CompositeInputFormat<NullWritable
				>();
			// create dummy TaskAttemptID
			TaskAttemptID tid = new TaskAttemptID("jt", 1, TaskType.Map, 0, 0);
			conf.Set(MRJobConfig.TaskAttemptId, tid.ToString());
			inputFormat.CreateRecordReader(inputFormat.GetSplits(Job.GetInstance(conf))[0], new 
				TaskAttemptContextImpl(conf, tid));
		}

		public class Fake_ClassLoader : ClassLoader
		{
		}

		public class IF_ClassLoaderChecker<K, V> : MapReduceTestUtil.Fake_IF<K, V>
		{
			public IF_ClassLoaderChecker()
			{
			}

			public override RecordReader<K, V> CreateRecordReader(InputSplit ignored, TaskAttemptContext
				 context)
			{
				return new TestWrappedRRClassloader.RR_ClassLoaderChecker<K, V>(context.GetConfiguration
					());
			}
		}

		public class RR_ClassLoaderChecker<K, V> : MapReduceTestUtil.Fake_RR<K, V>
		{
			public RR_ClassLoaderChecker(Configuration conf)
			{
				NUnit.Framework.Assert.IsTrue("The class loader has not been inherited from " + typeof(
					CompositeRecordReader).Name, conf.GetClassLoader() is TestWrappedRRClassloader.Fake_ClassLoader
					);
			}
		}
	}
}
