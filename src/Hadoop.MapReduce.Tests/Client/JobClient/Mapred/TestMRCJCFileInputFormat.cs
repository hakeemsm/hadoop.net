using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMRCJCFileInputFormat : TestCase
	{
		internal Configuration conf = new Configuration();

		internal MiniDFSCluster dfs = null;

		/// <exception cref="System.Exception"/>
		private MiniDFSCluster NewDFSCluster(JobConf conf)
		{
			return new MiniDFSCluster.Builder(conf).NumDataNodes(4).Racks(new string[] { "/rack0"
				, "/rack0", "/rack1", "/rack1" }).Hosts(new string[] { "host0", "host1", "host2"
				, "host3" }).Build();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocality()
		{
			JobConf job = new JobConf(conf);
			dfs = NewDFSCluster(job);
			FileSystem fs = dfs.GetFileSystem();
			System.Console.Out.WriteLine("FileSystem " + fs.GetUri());
			Path inputDir = new Path("/foo/");
			string fileName = "part-0000";
			CreateInputs(fs, inputDir, fileName);
			// split it using a file input format
			TextInputFormat.AddInputPath(job, inputDir);
			TextInputFormat inFormat = new TextInputFormat();
			inFormat.Configure(job);
			InputSplit[] splits = inFormat.GetSplits(job, 1);
			FileStatus fileStatus = fs.GetFileStatus(new Path(inputDir, fileName));
			BlockLocation[] locations = fs.GetFileBlockLocations(fileStatus, 0, fileStatus.GetLen
				());
			System.Console.Out.WriteLine("Made splits");
			// make sure that each split is a block and the locations match
			for (int i = 0; i < splits.Length; ++i)
			{
				FileSplit fileSplit = (FileSplit)splits[i];
				System.Console.Out.WriteLine("File split: " + fileSplit);
				foreach (string h in fileSplit.GetLocations())
				{
					System.Console.Out.WriteLine("Location: " + h);
				}
				System.Console.Out.WriteLine("Block: " + locations[i]);
				NUnit.Framework.Assert.AreEqual(locations[i].GetOffset(), fileSplit.GetStart());
				NUnit.Framework.Assert.AreEqual(locations[i].GetLength(), fileSplit.GetLength());
				string[] blockLocs = locations[i].GetHosts();
				string[] splitLocs = fileSplit.GetLocations();
				NUnit.Framework.Assert.AreEqual(2, blockLocs.Length);
				NUnit.Framework.Assert.AreEqual(2, splitLocs.Length);
				NUnit.Framework.Assert.IsTrue((blockLocs[0].Equals(splitLocs[0]) && blockLocs[1].
					Equals(splitLocs[1])) || (blockLocs[1].Equals(splitLocs[0]) && blockLocs[0].Equals
					(splitLocs[1])));
			}
			NUnit.Framework.Assert.AreEqual("Expected value of " + FileInputFormat.NumInputFiles
				, 1, job.GetLong(FileInputFormat.NumInputFiles, 0));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		private void CreateInputs(FileSystem fs, Path inDir, string fileName)
		{
			// create a multi-block file on hdfs
			Path path = new Path(inDir, fileName);
			short replication = 2;
			DataOutputStream @out = fs.Create(path, true, 4096, replication, 512, null);
			for (int i = 0; i < 1000; ++i)
			{
				@out.WriteChars("Hello\n");
			}
			@out.Close();
			System.Console.Out.WriteLine("Wrote file");
			DFSTestUtil.WaitReplication(fs, path, replication);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNumInputs()
		{
			JobConf job = new JobConf(conf);
			dfs = NewDFSCluster(job);
			FileSystem fs = dfs.GetFileSystem();
			System.Console.Out.WriteLine("FileSystem " + fs.GetUri());
			Path inputDir = new Path("/foo/");
			int numFiles = 10;
			string fileNameBase = "part-0000";
			for (int i = 0; i < numFiles; ++i)
			{
				CreateInputs(fs, inputDir, fileNameBase + i.ToString());
			}
			CreateInputs(fs, inputDir, "_meta");
			CreateInputs(fs, inputDir, "_temp");
			// split it using a file input format
			TextInputFormat.AddInputPath(job, inputDir);
			TextInputFormat inFormat = new TextInputFormat();
			inFormat.Configure(job);
			InputSplit[] splits = inFormat.GetSplits(job, 1);
			NUnit.Framework.Assert.AreEqual("Expected value of " + FileInputFormat.NumInputFiles
				, numFiles, job.GetLong(FileInputFormat.NumInputFiles, 0));
		}

		internal readonly Path root = new Path("/TestFileInputFormat");

		internal readonly Path file1;

		internal readonly Path dir1;

		internal readonly Path file2;

		internal const int Blocksize = 1024;

		internal static readonly byte[] databuf = new byte[Blocksize];

		private static readonly string[] rack1 = new string[] { "/r1" };

		private static readonly string[] hosts1 = new string[] { "host1.rack1.com" };

		private class DummyFileInputFormat : FileInputFormat<Text, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<Text, Text> GetRecordReader(InputSplit split, JobConf
				 job, Reporter reporter)
			{
				return null;
			}

			internal DummyFileInputFormat(TestMRCJCFileInputFormat _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMRCJCFileInputFormat _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultiLevelInput()
		{
			JobConf job = new JobConf(conf);
			job.SetBoolean("dfs.replication.considerLoad", false);
			dfs = new MiniDFSCluster.Builder(job).Racks(rack1).Hosts(hosts1).Build();
			dfs.WaitActive();
			string namenode = (dfs.GetFileSystem()).GetUri().GetHost() + ":" + (dfs.GetFileSystem
				()).GetUri().GetPort();
			FileSystem fileSys = dfs.GetFileSystem();
			if (!fileSys.Mkdirs(dir1))
			{
				throw new IOException("Mkdirs failed to create " + root.ToString());
			}
			WriteFile(job, file1, (short)1, 1);
			WriteFile(job, file2, (short)1, 1);
			// split it using a CombinedFile input format
			TestMRCJCFileInputFormat.DummyFileInputFormat inFormat = new TestMRCJCFileInputFormat.DummyFileInputFormat
				(this);
			TestMRCJCFileInputFormat.DummyFileInputFormat.SetInputPaths(job, root);
			// By default, we don't allow multi-level/recursive inputs
			bool exceptionThrown = false;
			try
			{
				InputSplit[] splits = inFormat.GetSplits(job, 1);
			}
			catch (Exception)
			{
				exceptionThrown = true;
			}
			NUnit.Framework.Assert.IsTrue("Exception should be thrown by default for scanning a "
				 + "directory with directories inside.", exceptionThrown);
			// Enable multi-level/recursive inputs
			job.SetBoolean(FileInputFormat.InputDirRecursive, true);
			InputSplit[] splits_1 = inFormat.GetSplits(job, 1);
			NUnit.Framework.Assert.AreEqual(splits_1.Length, 2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLastInputSplitAtSplitBoundary()
		{
			FileInputFormat fif = new TestMRCJCFileInputFormat.FileInputFormatForTest(this, 1024l
				 * 1024 * 1024, 128l * 1024 * 1024);
			JobConf job = new JobConf();
			InputSplit[] splits = fif.GetSplits(job, 8);
			NUnit.Framework.Assert.AreEqual(8, splits.Length);
			for (int i = 0; i < splits.Length; i++)
			{
				InputSplit split = splits[i];
				NUnit.Framework.Assert.AreEqual(("host" + i), split.GetLocations()[0]);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLastInputSplitExceedingSplitBoundary()
		{
			FileInputFormat fif = new TestMRCJCFileInputFormat.FileInputFormatForTest(this, 1027l
				 * 1024 * 1024, 128l * 1024 * 1024);
			JobConf job = new JobConf();
			InputSplit[] splits = fif.GetSplits(job, 8);
			NUnit.Framework.Assert.AreEqual(8, splits.Length);
			for (int i = 0; i < splits.Length; i++)
			{
				InputSplit split = splits[i];
				NUnit.Framework.Assert.AreEqual(("host" + i), split.GetLocations()[0]);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLastInputSplitSingleSplit()
		{
			FileInputFormat fif = new TestMRCJCFileInputFormat.FileInputFormatForTest(this, 100l
				 * 1024 * 1024, 128l * 1024 * 1024);
			JobConf job = new JobConf();
			InputSplit[] splits = fif.GetSplits(job, 1);
			NUnit.Framework.Assert.AreEqual(1, splits.Length);
			for (int i = 0; i < splits.Length; i++)
			{
				InputSplit split = splits[i];
				NUnit.Framework.Assert.AreEqual(("host" + i), split.GetLocations()[0]);
			}
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
			public override RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, 
				Reporter reporter)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override FileStatus[] ListStatus(JobConf job)
			{
				FileStatus mockFileStatus = Org.Mockito.Mockito.Mock<FileStatus>();
				Org.Mockito.Mockito.When(mockFileStatus.GetBlockSize()).ThenReturn(this.splitSize
					);
				Org.Mockito.Mockito.When(mockFileStatus.IsDirectory()).ThenReturn(false);
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
				FileStatus[] fs = new FileStatus[1];
				fs[0] = mockFileStatus;
				return fs;
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		internal static void WriteFile(Configuration conf, Path name, short replication, 
			int numBlocks)
		{
			FileSystem fileSys = FileSystem.Get(conf);
			FSDataOutputStream stm = fileSys.Create(name, true, conf.GetInt("io.file.buffer.size"
				, 4096), replication, (long)Blocksize);
			for (int i = 0; i < numBlocks; i++)
			{
				stm.Write(databuf);
			}
			stm.Close();
			DFSTestUtil.WaitReplication(fileSys, name, replication);
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			if (dfs != null)
			{
				dfs.Shutdown();
				dfs = null;
			}
		}

		public TestMRCJCFileInputFormat()
		{
			file1 = new Path(root, "file1");
			dir1 = new Path(root, "dir1");
			file2 = new Path(dir1, "file2");
		}
	}
}
