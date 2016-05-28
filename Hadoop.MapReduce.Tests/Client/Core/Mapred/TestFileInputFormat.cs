using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFileInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.TestFileInputFormat
			));

		private static string testTmpDir = Runtime.GetProperty("test.build.data", "/tmp");

		private static readonly Path TestRootDir = new Path(testTmpDir, "TestFIF");

		private static FileSystem localFs;

		private int numThreads;

		public TestFileInputFormat(int numThreads)
		{
			this.numThreads = numThreads;
			Log.Info("Running with numThreads: " + numThreads);
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			object[][] data = new object[][] { new object[] { 1 }, new object[] { 5 } };
			return Arrays.AsList(data);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			Log.Info("Using Test Dir: " + TestRootDir);
			localFs = FileSystem.GetLocal(new Configuration());
			localFs.Delete(TestRootDir, true);
			localFs.Mkdirs(TestRootDir);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			localFs.Delete(TestRootDir, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListLocatedStatus()
		{
			Configuration conf = GetConfiguration();
			conf.SetBoolean("fs.test.impl.disable.cache", false);
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			conf.Set(FileInputFormat.InputDir, "test:///a1/a2");
			TestFileInputFormat.MockFileSystem mockFs = (TestFileInputFormat.MockFileSystem)new 
				Path("test:///").GetFileSystem(conf);
			NUnit.Framework.Assert.AreEqual("listLocatedStatus already called", 0, mockFs.numListLocatedStatusCalls
				);
			JobConf job = new JobConf(conf);
			TextInputFormat fileInputFormat = new TextInputFormat();
			fileInputFormat.Configure(job);
			InputSplit[] splits = fileInputFormat.GetSplits(job, 1);
			NUnit.Framework.Assert.AreEqual("Input splits are not correct", 2, splits.Length);
			NUnit.Framework.Assert.AreEqual("listLocatedStatuss calls", 1, mockFs.numListLocatedStatusCalls
				);
			FileSystem.CloseAll();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitLocationInfo()
		{
			Configuration conf = GetConfiguration();
			conf.Set(FileInputFormat.InputDir, "test:///a1/a2");
			JobConf job = new JobConf(conf);
			TextInputFormat fileInputFormat = new TextInputFormat();
			fileInputFormat.Configure(job);
			FileSplit[] splits = (FileSplit[])fileInputFormat.GetSplits(job, 1);
			string[] locations = splits[0].GetLocations();
			NUnit.Framework.Assert.AreEqual(2, locations.Length);
			SplitLocationInfo[] locationInfo = splits[0].GetLocationInfo();
			NUnit.Framework.Assert.AreEqual(2, locationInfo.Length);
			SplitLocationInfo localhostInfo = locations[0].Equals("localhost") ? locationInfo
				[0] : locationInfo[1];
			SplitLocationInfo otherhostInfo = locations[0].Equals("otherhost") ? locationInfo
				[0] : locationInfo[1];
			NUnit.Framework.Assert.IsTrue(localhostInfo.IsOnDisk());
			NUnit.Framework.Assert.IsTrue(localhostInfo.IsInMemory());
			NUnit.Framework.Assert.IsTrue(otherhostInfo.IsOnDisk());
			NUnit.Framework.Assert.IsFalse(otherhostInfo.IsInMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusSimple()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			IList<Path> expectedPaths = Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat
				.ConfigureTestSimple(conf, localFs);
			JobConf jobConf = new JobConf(conf);
			TextInputFormat fif = new TextInputFormat();
			fif.Configure(jobConf);
			FileStatus[] statuses = fif.ListStatus(jobConf);
			Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat.VerifyFileStatuses(expectedPaths
				, Lists.NewArrayList(statuses), localFs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusNestedRecursive()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			IList<Path> expectedPaths = Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat
				.ConfigureTestNestedRecursive(conf, localFs);
			JobConf jobConf = new JobConf(conf);
			TextInputFormat fif = new TextInputFormat();
			fif.Configure(jobConf);
			FileStatus[] statuses = fif.ListStatus(jobConf);
			Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat.VerifyFileStatuses(expectedPaths
				, Lists.NewArrayList(statuses), localFs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusNestedNonRecursive()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			IList<Path> expectedPaths = Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat
				.ConfigureTestNestedNonRecursive(conf, localFs);
			JobConf jobConf = new JobConf(conf);
			TextInputFormat fif = new TextInputFormat();
			fif.Configure(jobConf);
			FileStatus[] statuses = fif.ListStatus(jobConf);
			Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat.VerifyFileStatuses(expectedPaths
				, Lists.NewArrayList(statuses), localFs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusErrorOnNonExistantDir()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat.ConfigureTestErrorOnNonExistantDir
				(conf, localFs);
			JobConf jobConf = new JobConf(conf);
			TextInputFormat fif = new TextInputFormat();
			fif.Configure(jobConf);
			try
			{
				fif.ListStatus(jobConf);
				NUnit.Framework.Assert.Fail("Expecting an IOException for a missing Input path");
			}
			catch (IOException e)
			{
				Path expectedExceptionPath = new Path(TestRootDir, "input2");
				expectedExceptionPath = localFs.MakeQualified(expectedExceptionPath);
				NUnit.Framework.Assert.IsTrue(e is InvalidInputException);
				NUnit.Framework.Assert.AreEqual("Input path does not exist: " + expectedExceptionPath
					.ToString(), e.Message);
			}
		}

		private Configuration GetConfiguration()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.test.impl.disable.cache", "true");
			conf.SetClass("fs.test.impl", typeof(TestFileInputFormat.MockFileSystem), typeof(
				FileSystem));
			conf.Set(FileInputFormat.InputDir, "test:///a1");
			return conf;
		}

		internal class MockFileSystem : RawLocalFileSystem
		{
			internal int numListLocatedStatusCalls = 0;

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				if (f.ToString().Equals("test:/a1"))
				{
					return new FileStatus[] { new FileStatus(0, true, 1, 150, 150, new Path("test:/a1/a2"
						)), new FileStatus(10, false, 1, 150, 150, new Path("test:/a1/file1")) };
				}
				else
				{
					if (f.ToString().Equals("test:/a1/a2"))
					{
						return new FileStatus[] { new FileStatus(10, false, 1, 150, 150, new Path("test:/a1/a2/file2"
							)), new FileStatus(10, false, 1, 151, 150, new Path("test:/a1/a2/file3")) };
					}
				}
				return new FileStatus[0];
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] GlobStatus(Path pathPattern, PathFilter filter)
			{
				return new FileStatus[] { new FileStatus(10, true, 1, 150, 150, pathPattern) };
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f, PathFilter filter)
			{
				return this.ListStatus(f);
			}

			/// <exception cref="System.IO.IOException"/>
			public override BlockLocation[] GetFileBlockLocations(Path p, long start, long len
				)
			{
				return new BlockLocation[] { new BlockLocation(new string[] { "localhost:50010", 
					"otherhost:50010" }, new string[] { "localhost", "otherhost" }, new string[] { "localhost"
					 }, new string[0], 0, len, false) };
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path f, PathFilter
				 filter)
			{
				++numListLocatedStatusCalls;
				return base.ListLocatedStatus(f, filter);
			}
		}
	}
}
