using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Test;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestMRCJCFileInputFormat
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAddInputPath()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.defaultFS", "file:///abc/");
			Job j = Job.GetInstance(conf);
			//setup default fs
			FileSystem defaultfs = FileSystem.Get(conf);
			System.Console.Out.WriteLine("defaultfs.getUri() = " + defaultfs.GetUri());
			{
				//test addInputPath
				Path original = new Path("file:/foo");
				System.Console.Out.WriteLine("original = " + original);
				FileInputFormat.AddInputPath(j, original);
				Path[] results = FileInputFormat.GetInputPaths(j);
				System.Console.Out.WriteLine("results = " + Arrays.AsList(results));
				NUnit.Framework.Assert.AreEqual(1, results.Length);
				NUnit.Framework.Assert.AreEqual(original, results[0]);
			}
			{
				//test setInputPaths
				Path original = new Path("file:/bar");
				System.Console.Out.WriteLine("original = " + original);
				FileInputFormat.SetInputPaths(j, original);
				Path[] results = FileInputFormat.GetInputPaths(j);
				System.Console.Out.WriteLine("results = " + Arrays.AsList(results));
				NUnit.Framework.Assert.AreEqual(1, results.Length);
				NUnit.Framework.Assert.AreEqual(original, results[0]);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumInputFiles()
		{
			Configuration conf = Org.Mockito.Mockito.Spy(new Configuration());
			Job job = MockitoMaker.Make(MockitoMaker.Stub<Job>().Returning(conf).from.GetConfiguration
				());
			FileStatus stat = MockitoMaker.Make(MockitoMaker.Stub<FileStatus>().Returning(0L)
				.from.GetLen());
			TextInputFormat ispy = Org.Mockito.Mockito.Spy(new TextInputFormat());
			Org.Mockito.Mockito.DoReturn(Arrays.AsList(stat)).When(ispy).ListStatus(job);
			ispy.GetSplits(job);
			Org.Mockito.Mockito.Verify(conf).SetLong(FileInputFormat.NumInputFiles, 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLastInputSplitAtSplitBoundary()
		{
			FileInputFormat fif = new TestMRCJCFileInputFormat.FileInputFormatForTest(this, 1024l
				 * 1024 * 1024, 128l * 1024 * 1024);
			Configuration conf = new Configuration();
			JobContext jobContext = Org.Mockito.Mockito.Mock<JobContext>();
			Org.Mockito.Mockito.When(jobContext.GetConfiguration()).ThenReturn(conf);
			IList<InputSplit> splits = fif.GetSplits(jobContext);
			NUnit.Framework.Assert.AreEqual(8, splits.Count);
			for (int i = 0; i < splits.Count; i++)
			{
				InputSplit split = splits[i];
				NUnit.Framework.Assert.AreEqual(("host" + i), split.GetLocations()[0]);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLastInputSplitExceedingSplitBoundary()
		{
			FileInputFormat fif = new TestMRCJCFileInputFormat.FileInputFormatForTest(this, 1027l
				 * 1024 * 1024, 128l * 1024 * 1024);
			Configuration conf = new Configuration();
			JobContext jobContext = Org.Mockito.Mockito.Mock<JobContext>();
			Org.Mockito.Mockito.When(jobContext.GetConfiguration()).ThenReturn(conf);
			IList<InputSplit> splits = fif.GetSplits(jobContext);
			NUnit.Framework.Assert.AreEqual(8, splits.Count);
			for (int i = 0; i < splits.Count; i++)
			{
				InputSplit split = splits[i];
				NUnit.Framework.Assert.AreEqual(("host" + i), split.GetLocations()[0]);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLastInputSplitSingleSplit()
		{
			FileInputFormat fif = new TestMRCJCFileInputFormat.FileInputFormatForTest(this, 100l
				 * 1024 * 1024, 128l * 1024 * 1024);
			Configuration conf = new Configuration();
			JobContext jobContext = Org.Mockito.Mockito.Mock<JobContext>();
			Org.Mockito.Mockito.When(jobContext.GetConfiguration()).ThenReturn(conf);
			IList<InputSplit> splits = fif.GetSplits(jobContext);
			NUnit.Framework.Assert.AreEqual(1, splits.Count);
			for (int i = 0; i < splits.Count; i++)
			{
				InputSplit split = splits[i];
				NUnit.Framework.Assert.AreEqual(("host" + i), split.GetLocations()[0]);
			}
		}

		/// <summary>Test when the input file's length is 0.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestForEmptyFile()
		{
			Configuration conf = new Configuration();
			FileSystem fileSys = FileSystem.Get(conf);
			Path file = new Path("test" + "/file");
			FSDataOutputStream @out = fileSys.Create(file, true, conf.GetInt("io.file.buffer.size"
				, 4096), (short)1, (long)1024);
			@out.Write(new byte[0]);
			@out.Close();
			// split it using a File input format
			TestMRCJCFileInputFormat.DummyInputFormat inFormat = new TestMRCJCFileInputFormat.DummyInputFormat
				(this);
			Job job = Job.GetInstance(conf);
			FileInputFormat.SetInputPaths(job, "test");
			IList<InputSplit> splits = inFormat.GetSplits(job);
			NUnit.Framework.Assert.AreEqual(1, splits.Count);
			FileSplit fileSplit = (FileSplit)splits[0];
			NUnit.Framework.Assert.AreEqual(0, fileSplit.GetLocations().Length);
			NUnit.Framework.Assert.AreEqual(file.GetName(), fileSplit.GetPath().GetName());
			NUnit.Framework.Assert.AreEqual(0, fileSplit.GetStart());
			NUnit.Framework.Assert.AreEqual(0, fileSplit.GetLength());
			fileSys.Delete(file.GetParent(), true);
		}

		/// <summary>Dummy class to extend FileInputFormat</summary>
		private class DummyInputFormat : FileInputFormat<Text, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				return null;
			}

			internal DummyInputFormat(TestMRCJCFileInputFormat _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMRCJCFileInputFormat _enclosing;
		}

		private class FileInputFormatForTest<K, V> : FileInputFormat<K, V>
		{
			internal long splitSize;

			internal long length;

			internal FileInputFormatForTest(TestMRCJCFileInputFormat _enclosing, long length, 
				long splitSize)
			{
				this._enclosing = _enclosing;
				this.length = length;
				this.splitSize = splitSize;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override IList<FileStatus> ListStatus(JobContext job)
			{
				FileStatus mockFileStatus = Org.Mockito.Mockito.Mock<FileStatus>();
				Org.Mockito.Mockito.When(mockFileStatus.GetBlockSize()).ThenReturn(this.splitSize
					);
				Path mockPath = Org.Mockito.Mockito.Mock<Path>();
				FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
				BlockLocation[] blockLocations = this.MockBlockLocations(this.length, this.splitSize
					);
				Org.Mockito.Mockito.When(mockFs.GetFileBlockLocations(mockFileStatus, 0, this.length
					)).ThenReturn(blockLocations);
				Org.Mockito.Mockito.When(mockPath.GetFileSystem(Matchers.Any<Configuration>())).ThenReturn
					(mockFs);
				Org.Mockito.Mockito.When(mockFileStatus.GetPath()).ThenReturn(mockPath);
				Org.Mockito.Mockito.When(mockFileStatus.GetLen()).ThenReturn(this.length);
				IList<FileStatus> list = new AList<FileStatus>();
				list.AddItem(mockFileStatus);
				return list;
			}

			protected override long ComputeSplitSize(long blockSize, long minSize, long maxSize
				)
			{
				return this.splitSize;
			}

			private BlockLocation[] MockBlockLocations(long size, long splitSize)
			{
				int numLocations = (int)(size / splitSize);
				if (size % splitSize != 0)
				{
					numLocations++;
				}
				BlockLocation[] blockLocations = new BlockLocation[numLocations];
				for (int i = 0; i < numLocations; i++)
				{
					string[] names = new string[] { "b" + i };
					string[] hosts = new string[] { "host" + i };
					blockLocations[i] = new BlockLocation(names, hosts, i * splitSize, Math.Min(splitSize
						, size - (splitSize * i)));
				}
				return blockLocations;
			}

			private readonly TestMRCJCFileInputFormat _enclosing;
		}
	}
}
