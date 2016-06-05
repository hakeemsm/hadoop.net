using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	public class TestWrappedRecordReaderClassloader : TestCase
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
			JobConf job = new JobConf();
			TestWrappedRecordReaderClassloader.Fake_ClassLoader classLoader = new TestWrappedRecordReaderClassloader.Fake_ClassLoader
				();
			job.SetClassLoader(classLoader);
			NUnit.Framework.Assert.IsTrue(job.GetClassLoader() is TestWrappedRecordReaderClassloader.Fake_ClassLoader
				);
			FileSystem fs = FileSystem.Get(job);
			Path testdir = new Path(Runtime.GetProperty("test.build.data", "/tmp")).MakeQualified
				(fs);
			Path @base = new Path(testdir, "/empty");
			Path[] src = new Path[] { new Path(@base, "i0"), new Path("i1"), new Path("i2") };
			job.Set("mapreduce.join.expr", CompositeInputFormat.Compose("outer", typeof(TestWrappedRecordReaderClassloader.IF_ClassLoaderChecker
				), src));
			CompositeInputFormat<NullWritable> inputFormat = new CompositeInputFormat<NullWritable
				>();
			inputFormat.GetRecordReader(inputFormat.GetSplits(job, 1)[0], job, Reporter.Null);
		}

		public class Fake_ClassLoader : ClassLoader
		{
		}

		public class IF_ClassLoaderChecker<K, V> : InputFormat<K, V>, JobConfigurable
		{
			public class FakeSplit : InputSplit
			{
				/// <exception cref="System.IO.IOException"/>
				public virtual void Write(DataOutput @out)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void ReadFields(DataInput @in)
				{
				}

				public virtual long GetLength()
				{
					return 0L;
				}

				public virtual string[] GetLocations()
				{
					return new string[0];
				}
			}

			public static void SetKeyClass(JobConf job, Type k)
			{
				job.SetClass("test.fakeif.keyclass", k, typeof(WritableComparable));
			}

			public static void SetValClass(JobConf job, Type v)
			{
				job.SetClass("test.fakeif.valclass", v, typeof(Writable));
			}

			protected internal Type keyclass;

			protected internal Type valclass;

			public virtual void Configure(JobConf job)
			{
				keyclass = (Type)job.GetClass<WritableComparable>("test.fakeif.keyclass", typeof(
					NullWritable));
				valclass = (Type)job.GetClass<WritableComparable>("test.fakeif.valclass", typeof(
					NullWritable));
			}

			public IF_ClassLoaderChecker()
			{
			}

			public virtual InputSplit[] GetSplits(JobConf conf, int splits)
			{
				return new InputSplit[] { new TestWrappedRecordReaderClassloader.IF_ClassLoaderChecker.FakeSplit
					() };
			}

			public virtual RecordReader<K, V> GetRecordReader(InputSplit ignored, JobConf job
				, Reporter reporter)
			{
				return new TestWrappedRecordReaderClassloader.RR_ClassLoaderChecker<K, V>(job);
			}
		}

		public class RR_ClassLoaderChecker<K, V> : RecordReader<K, V>
		{
			private Type keyclass;

			private Type valclass;

			public RR_ClassLoaderChecker(JobConf job)
			{
				NUnit.Framework.Assert.IsTrue("The class loader has not been inherited from " + typeof(
					CompositeRecordReader).Name, job.GetClassLoader() is TestWrappedRecordReaderClassloader.Fake_ClassLoader
					);
				keyclass = (Type)job.GetClass<WritableComparable>("test.fakeif.keyclass", typeof(
					NullWritable));
				valclass = (Type)job.GetClass<WritableComparable>("test.fakeif.valclass", typeof(
					NullWritable));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(K key, V value)
			{
				return false;
			}

			public virtual K CreateKey()
			{
				return ReflectionUtils.NewInstance(keyclass, null);
			}

			public virtual V CreateValue()
			{
				return ReflectionUtils.NewInstance(valclass, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return 0L;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual float GetProgress()
			{
				return 0.0f;
			}
		}
	}
}
