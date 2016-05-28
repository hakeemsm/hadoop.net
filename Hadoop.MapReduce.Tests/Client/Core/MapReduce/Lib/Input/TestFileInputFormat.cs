using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestFileInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Input.TestFileInputFormat
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
		public virtual void TestNumInputFilesRecursively()
		{
			Configuration conf = GetConfiguration();
			conf.Set(FileInputFormat.InputDirRecursive, "true");
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fileInputFormat = new TextInputFormat();
			IList<InputSplit> splits = fileInputFormat.GetSplits(job);
			NUnit.Framework.Assert.AreEqual("Input splits are not correct", 3, splits.Count);
			VerifySplits(Lists.NewArrayList("test:/a1/a2/file2", "test:/a1/a2/file3", "test:/a1/file1"
				), splits);
			// Using the deprecated configuration
			conf = GetConfiguration();
			conf.Set("mapred.input.dir.recursive", "true");
			job = Job.GetInstance(conf);
			splits = fileInputFormat.GetSplits(job);
			VerifySplits(Lists.NewArrayList("test:/a1/a2/file2", "test:/a1/a2/file3", "test:/a1/file1"
				), splits);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumInputFilesWithoutRecursively()
		{
			Configuration conf = GetConfiguration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fileInputFormat = new TextInputFormat();
			IList<InputSplit> splits = fileInputFormat.GetSplits(job);
			NUnit.Framework.Assert.AreEqual("Input splits are not correct", 2, splits.Count);
			VerifySplits(Lists.NewArrayList("test:/a1/a2", "test:/a1/file1"), splits);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListLocatedStatus()
		{
			Configuration conf = GetConfiguration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			conf.SetBoolean("fs.test.impl.disable.cache", false);
			conf.Set(FileInputFormat.InputDir, "test:///a1/a2");
			TestFileInputFormat.MockFileSystem mockFs = (TestFileInputFormat.MockFileSystem)new 
				Path("test:///").GetFileSystem(conf);
			NUnit.Framework.Assert.AreEqual("listLocatedStatus already called", 0, mockFs.numListLocatedStatusCalls
				);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fileInputFormat = new TextInputFormat();
			IList<InputSplit> splits = fileInputFormat.GetSplits(job);
			NUnit.Framework.Assert.AreEqual("Input splits are not correct", 2, splits.Count);
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
			Job job = Job.GetInstance(conf);
			TextInputFormat fileInputFormat = new TextInputFormat();
			IList<InputSplit> splits = fileInputFormat.GetSplits(job);
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
			IList<Path> expectedPaths = ConfigureTestSimple(conf, localFs);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fif = new TextInputFormat();
			IList<FileStatus> statuses = fif.ListStatus(job);
			VerifyFileStatuses(expectedPaths, statuses, localFs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusNestedRecursive()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			IList<Path> expectedPaths = ConfigureTestNestedRecursive(conf, localFs);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fif = new TextInputFormat();
			IList<FileStatus> statuses = fif.ListStatus(job);
			VerifyFileStatuses(expectedPaths, statuses, localFs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusNestedNonRecursive()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			IList<Path> expectedPaths = ConfigureTestNestedNonRecursive(conf, localFs);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fif = new TextInputFormat();
			IList<FileStatus> statuses = fif.ListStatus(job);
			VerifyFileStatuses(expectedPaths, statuses, localFs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusErrorOnNonExistantDir()
		{
			Configuration conf = new Configuration();
			conf.SetInt(FileInputFormat.ListStatusNumThreads, numThreads);
			ConfigureTestErrorOnNonExistantDir(conf, localFs);
			Job job = Job.GetInstance(conf);
			FileInputFormat<object, object> fif = new TextInputFormat();
			try
			{
				fif.ListStatus(job);
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

		/// <exception cref="System.IO.IOException"/>
		public static IList<Path> ConfigureTestSimple(Configuration conf, FileSystem localFs
			)
		{
			Path base1 = new Path(TestRootDir, "input1");
			Path base2 = new Path(TestRootDir, "input2");
			conf.Set(FileInputFormat.InputDir, localFs.MakeQualified(base1) + "," + localFs.MakeQualified
				(base2));
			localFs.Mkdirs(base1);
			localFs.Mkdirs(base2);
			Path in1File1 = new Path(base1, "file1");
			Path in1File2 = new Path(base1, "file2");
			localFs.CreateNewFile(in1File1);
			localFs.CreateNewFile(in1File2);
			Path in2File1 = new Path(base2, "file1");
			Path in2File2 = new Path(base2, "file2");
			localFs.CreateNewFile(in2File1);
			localFs.CreateNewFile(in2File2);
			IList<Path> expectedPaths = Lists.NewArrayList(in1File1, in1File2, in2File1, in2File2
				);
			return expectedPaths;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<Path> ConfigureTestNestedRecursive(Configuration conf, FileSystem
			 localFs)
		{
			Path base1 = new Path(TestRootDir, "input1");
			conf.Set(FileInputFormat.InputDir, localFs.MakeQualified(base1).ToString());
			conf.SetBoolean(FileInputFormat.InputDirRecursive, true);
			localFs.Mkdirs(base1);
			Path inDir1 = new Path(base1, "dir1");
			Path inDir2 = new Path(base1, "dir2");
			Path inFile1 = new Path(base1, "file1");
			Path dir1File1 = new Path(inDir1, "file1");
			Path dir1File2 = new Path(inDir1, "file2");
			Path dir2File1 = new Path(inDir2, "file1");
			Path dir2File2 = new Path(inDir2, "file2");
			localFs.Mkdirs(inDir1);
			localFs.Mkdirs(inDir2);
			localFs.CreateNewFile(inFile1);
			localFs.CreateNewFile(dir1File1);
			localFs.CreateNewFile(dir1File2);
			localFs.CreateNewFile(dir2File1);
			localFs.CreateNewFile(dir2File2);
			IList<Path> expectedPaths = Lists.NewArrayList(inFile1, dir1File1, dir1File2, dir2File1
				, dir2File2);
			return expectedPaths;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<Path> ConfigureTestNestedNonRecursive(Configuration conf, FileSystem
			 localFs)
		{
			Path base1 = new Path(TestRootDir, "input1");
			conf.Set(FileInputFormat.InputDir, localFs.MakeQualified(base1).ToString());
			conf.SetBoolean(FileInputFormat.InputDirRecursive, false);
			localFs.Mkdirs(base1);
			Path inDir1 = new Path(base1, "dir1");
			Path inDir2 = new Path(base1, "dir2");
			Path inFile1 = new Path(base1, "file1");
			Path dir1File1 = new Path(inDir1, "file1");
			Path dir1File2 = new Path(inDir1, "file2");
			Path dir2File1 = new Path(inDir2, "file1");
			Path dir2File2 = new Path(inDir2, "file2");
			localFs.Mkdirs(inDir1);
			localFs.Mkdirs(inDir2);
			localFs.CreateNewFile(inFile1);
			localFs.CreateNewFile(dir1File1);
			localFs.CreateNewFile(dir1File2);
			localFs.CreateNewFile(dir2File1);
			localFs.CreateNewFile(dir2File2);
			IList<Path> expectedPaths = Lists.NewArrayList(inFile1, inDir1, inDir2);
			return expectedPaths;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<Path> ConfigureTestErrorOnNonExistantDir(Configuration conf, 
			FileSystem localFs)
		{
			Path base1 = new Path(TestRootDir, "input1");
			Path base2 = new Path(TestRootDir, "input2");
			conf.Set(FileInputFormat.InputDir, localFs.MakeQualified(base1) + "," + localFs.MakeQualified
				(base2));
			conf.SetBoolean(FileInputFormat.InputDirRecursive, true);
			localFs.Mkdirs(base1);
			Path inFile1 = new Path(base1, "file1");
			Path inFile2 = new Path(base1, "file2");
			localFs.CreateNewFile(inFile1);
			localFs.CreateNewFile(inFile2);
			IList<Path> expectedPaths = Lists.NewArrayList();
			return expectedPaths;
		}

		public static void VerifyFileStatuses(IList<Path> expectedPaths, IList<FileStatus
			> fetchedStatuses, FileSystem localFs)
		{
			NUnit.Framework.Assert.AreEqual(expectedPaths.Count, fetchedStatuses.Count);
			IEnumerable<Path> fqExpectedPaths = Iterables.Transform(expectedPaths, new _Function_344
				(localFs));
			ICollection<Path> expectedPathSet = Sets.NewHashSet(fqExpectedPaths);
			foreach (FileStatus fileStatus in fetchedStatuses)
			{
				if (!expectedPathSet.Remove(localFs.MakeQualified(fileStatus.GetPath())))
				{
					NUnit.Framework.Assert.Fail("Found extra fetched status: " + fileStatus.GetPath()
						);
				}
			}
			NUnit.Framework.Assert.AreEqual("Not all expectedPaths matched: " + expectedPathSet
				.ToString(), 0, expectedPathSet.Count);
		}

		private sealed class _Function_344 : Function<Path, Path>
		{
			public _Function_344(FileSystem localFs)
			{
				this.localFs = localFs;
			}

			public Path Apply(Path input)
			{
				return localFs.MakeQualified(input);
			}

			private readonly FileSystem localFs;
		}

		private void VerifySplits(IList<string> expected, IList<InputSplit> splits)
		{
			IEnumerable<string> pathsFromSplits = Iterables.Transform(splits, new _Function_365
				());
			ICollection<string> expectedSet = Sets.NewHashSet(expected);
			foreach (string splitPathString in pathsFromSplits)
			{
				if (!expectedSet.Remove(splitPathString))
				{
					NUnit.Framework.Assert.Fail("Found extra split: " + splitPathString);
				}
			}
			NUnit.Framework.Assert.AreEqual("Not all expectedPaths matched: " + expectedSet.ToString
				(), 0, expectedSet.Count);
		}

		private sealed class _Function_365 : Function<InputSplit, string>
		{
			public _Function_365()
			{
			}

			public string Apply(InputSplit input)
			{
				return ((FileSplit)input).GetPath().ToString();
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
