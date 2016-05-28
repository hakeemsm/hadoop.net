using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestCombineFileInputFormat
	{
		private static readonly string[] rack1 = new string[] { "/r1" };

		private static readonly string[] hosts1 = new string[] { "host1.rack1.com" };

		private static readonly string[] rack2 = new string[] { "/r2" };

		private static readonly string[] hosts2 = new string[] { "host2.rack2.com" };

		private static readonly string[] rack3 = new string[] { "/r3" };

		private static readonly string[] hosts3 = new string[] { "host3.rack3.com" };

		internal readonly Path inDir = new Path("/racktesting");

		internal readonly Path outputPath = new Path("/output");

		internal readonly Path dir1;

		internal readonly Path dir2;

		internal readonly Path dir3;

		internal readonly Path dir4;

		internal readonly Path dir5;

		internal const int Blocksize = 1024;

		internal static readonly byte[] databuf = new byte[Blocksize];

		private const string DummyFsUri = "dummyfs:///";

		/// <summary>Dummy class to extend CombineFileInputFormat</summary>
		private class DummyInputFormat : CombineFileInputFormat<Text, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				return null;
			}

			internal DummyInputFormat(TestCombineFileInputFormat _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestCombineFileInputFormat _enclosing;
		}

		/// <summary>Dummy class to extend CombineFileInputFormat.</summary>
		/// <remarks>
		/// Dummy class to extend CombineFileInputFormat. It allows
		/// non-existent files to be passed into the CombineFileInputFormat, allows
		/// for easy testing without having to create real files.
		/// </remarks>
		private class DummyInputFormat1 : TestCombineFileInputFormat.DummyInputFormat
		{
			/// <exception cref="System.IO.IOException"/>
			protected override IList<FileStatus> ListStatus(JobContext job)
			{
				Path[] files = FileInputFormat.GetInputPaths(job);
				IList<FileStatus> results = new AList<FileStatus>();
				for (int i = 0; i < files.Length; i++)
				{
					Path p = files[i];
					FileSystem fs = p.GetFileSystem(job.GetConfiguration());
					results.AddItem(fs.GetFileStatus(p));
				}
				return results;
			}

			internal DummyInputFormat1(TestCombineFileInputFormat _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestCombineFileInputFormat _enclosing;
		}

		/// <summary>Dummy class to extend CombineFileInputFormat.</summary>
		/// <remarks>
		/// Dummy class to extend CombineFileInputFormat. It allows
		/// testing with files having missing blocks without actually removing replicas.
		/// </remarks>
		public class MissingBlockFileSystem : DistributedFileSystem
		{
			internal string fileWithMissingBlocks;

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI name, Configuration conf)
			{
				fileWithMissingBlocks = string.Empty;
				base.Initialize(name, conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public override BlockLocation[] GetFileBlockLocations(FileStatus stat, long start
				, long len)
			{
				if (stat.IsDir())
				{
					return null;
				}
				System.Console.Out.WriteLine("File " + stat.GetPath());
				string name = stat.GetPath().ToUri().GetPath();
				BlockLocation[] locs = base.GetFileBlockLocations(stat, start, len);
				if (name.Equals(fileWithMissingBlocks))
				{
					System.Console.Out.WriteLine("Returning missing blocks for " + fileWithMissingBlocks
						);
					locs[0] = new HdfsBlockLocation(new BlockLocation(new string[0], new string[0], locs
						[0].GetOffset(), locs[0].GetLength()), null);
				}
				return locs;
			}

			public virtual void SetFileWithMissingBlocks(string f)
			{
				fileWithMissingBlocks = f;
			}
		}

		private const string DummyKey = "dummy.rr.key";

		private class DummyRecordReader : RecordReader<Text, Text>
		{
			private TaskAttemptContext context;

			private CombineFileSplit s;

			private int idx;

			private bool used;

			public DummyRecordReader(CombineFileSplit split, TaskAttemptContext context, int 
				i)
			{
				this.context = context;
				this.idx = i;
				this.s = split;
				this.used = true;
			}

			/// <returns>
			/// a value specified in the context to check whether the
			/// context is properly updated by the initialize() method.
			/// </returns>
			public virtual string GetDummyConfVal()
			{
				return this.context.GetConfiguration().Get(DummyKey);
			}

			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
				this.context = context;
				this.s = (CombineFileSplit)split;
				// By setting used to true in the c'tor, but false in initialize,
				// we can check that initialize() is always called before use
				// (e.g., in testReinit()).
				this.used = false;
			}

			public override bool NextKeyValue()
			{
				bool ret = !used;
				this.used = true;
				return ret;
			}

			public override Text GetCurrentKey()
			{
				return new Text(this.context.GetConfiguration().Get(DummyKey));
			}

			public override Text GetCurrentValue()
			{
				return new Text(this.s.GetPath(idx).ToString());
			}

			public override float GetProgress()
			{
				return used ? 1.0f : 0.0f;
			}

			public override void Close()
			{
			}
		}

		/// <summary>Extend CFIF to use CFRR with DummyRecordReader</summary>
		private class ChildRRInputFormat : CombineFileInputFormat<Text, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				return new CombineFileRecordReader((CombineFileSplit)split, context, (Type)typeof(
					TestCombineFileInputFormat.DummyRecordReader));
			}

			internal ChildRRInputFormat(TestCombineFileInputFormat _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestCombineFileInputFormat _enclosing;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecordReaderInit()
		{
			// Test that we properly initialize the child recordreader when
			// CombineFileInputFormat and CombineFileRecordReader are used.
			TaskAttemptID taskId = new TaskAttemptID("jt", 0, TaskType.Map, 0, 0);
			Configuration conf1 = new Configuration();
			conf1.Set(DummyKey, "STATE1");
			TaskAttemptContext context1 = new TaskAttemptContextImpl(conf1, taskId);
			// This will create a CombineFileRecordReader that itself contains a
			// DummyRecordReader.
			InputFormat inputFormat = new TestCombineFileInputFormat.ChildRRInputFormat(this);
			Path[] files = new Path[] { new Path("file1") };
			long[] lengths = new long[] { 1 };
			CombineFileSplit split = new CombineFileSplit(files, lengths);
			RecordReader rr = inputFormat.CreateRecordReader(split, context1);
			NUnit.Framework.Assert.IsTrue("Unexpected RR type!", rr is CombineFileRecordReader
				);
			// Verify that the initial configuration is the one being used.
			// Right after construction the dummy key should have value "STATE1"
			NUnit.Framework.Assert.AreEqual("Invalid initial dummy key value", "STATE1", rr.GetCurrentKey
				().ToString());
			// Switch the active context for the RecordReader...
			Configuration conf2 = new Configuration();
			conf2.Set(DummyKey, "STATE2");
			TaskAttemptContext context2 = new TaskAttemptContextImpl(conf2, taskId);
			rr.Initialize(split, context2);
			// And verify that the new context is updated into the child record reader.
			NUnit.Framework.Assert.AreEqual("Invalid secondary dummy key value", "STATE2", rr
				.GetCurrentKey().ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReinit()
		{
			// Test that a split containing multiple files works correctly,
			// with the child RecordReader getting its initialize() method
			// called a second time.
			TaskAttemptID taskId = new TaskAttemptID("jt", 0, TaskType.Map, 0, 0);
			Configuration conf = new Configuration();
			TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskId);
			// This will create a CombineFileRecordReader that itself contains a
			// DummyRecordReader.
			InputFormat inputFormat = new TestCombineFileInputFormat.ChildRRInputFormat(this);
			Path[] files = new Path[] { new Path("file1"), new Path("file2") };
			long[] lengths = new long[] { 1, 1 };
			CombineFileSplit split = new CombineFileSplit(files, lengths);
			RecordReader rr = inputFormat.CreateRecordReader(split, context);
			NUnit.Framework.Assert.IsTrue("Unexpected RR type!", rr is CombineFileRecordReader
				);
			// first initialize() call comes from MapTask. We'll do it here.
			rr.Initialize(split, context);
			// First value is first filename.
			NUnit.Framework.Assert.IsTrue(rr.NextKeyValue());
			NUnit.Framework.Assert.AreEqual("file1", rr.GetCurrentValue().ToString());
			// The inner RR will return false, because it only emits one (k, v) pair.
			// But there's another sub-split to process. This returns true to us.
			NUnit.Framework.Assert.IsTrue(rr.NextKeyValue());
			// And the 2nd rr will have its initialize method called correctly.
			NUnit.Framework.Assert.AreEqual("file2", rr.GetCurrentValue().ToString());
			// But after both child RR's have returned their singleton (k, v), this
			// should also return false.
			NUnit.Framework.Assert.IsFalse(rr.NextKeyValue());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitPlacement()
		{
			MiniDFSCluster dfs = null;
			FileSystem fileSys = null;
			try
			{
				/* Start 3 datanodes, one each in rack r1, r2, r3. Create five files
				* 1) file1 and file5, just after starting the datanode on r1, with
				*    a repl factor of 1, and,
				* 2) file2, just after starting the datanode on r2, with
				*    a repl factor of 2, and,
				* 3) file3, file4 after starting the all three datanodes, with a repl
				*    factor of 3.
				* At the end, file1, file5 will be present on only datanode1, file2 will
				* be present on datanode 1 and datanode2 and
				* file3, file4 will be present on all datanodes.
				*/
				Configuration conf = new Configuration();
				conf.SetBoolean("dfs.replication.considerLoad", false);
				dfs = new MiniDFSCluster.Builder(conf).Racks(rack1).Hosts(hosts1).Build();
				dfs.WaitActive();
				fileSys = dfs.GetFileSystem();
				if (!fileSys.Mkdirs(inDir))
				{
					throw new IOException("Mkdirs failed to create " + inDir.ToString());
				}
				Path file1 = new Path(dir1 + "/file1");
				WriteFile(conf, file1, (short)1, 1);
				// create another file on the same datanode
				Path file5 = new Path(dir5 + "/file5");
				WriteFile(conf, file5, (short)1, 1);
				// split it using a CombinedFile input format
				TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
					(this);
				Job job = Job.GetInstance(conf);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir5);
				IList<InputSplit> splits = inFormat.GetSplits(job);
				System.Console.Out.WriteLine("Made splits(Test0): " + splits.Count);
				foreach (InputSplit split in splits)
				{
					System.Console.Out.WriteLine("File split(Test0): " + split);
				}
				NUnit.Framework.Assert.AreEqual(1, splits.Count);
				CombineFileSplit fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file5.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				dfs.StartDataNodes(conf, 1, true, null, rack2, hosts2, null);
				dfs.WaitActive();
				// create file on two datanodes.
				Path file2 = new Path(dir2 + "/file2");
				WriteFile(conf, file2, (short)2, 2);
				// split it using a CombinedFile input format
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2);
				inFormat.SetMinSplitSizeRack(Blocksize);
				splits = inFormat.GetSplits(job);
				System.Console.Out.WriteLine("Made splits(Test1): " + splits.Count);
				// make sure that each split has different locations
				foreach (InputSplit split_1 in splits)
				{
					System.Console.Out.WriteLine("File split(Test1): " + split_1);
				}
				NUnit.Framework.Assert.AreEqual(2, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// create another file on 3 datanodes and 3 racks.
				dfs.StartDataNodes(conf, 1, true, null, rack3, hosts3, null);
				dfs.WaitActive();
				Path file3 = new Path(dir3 + "/file3");
				WriteFile(conf, new Path(dir3 + "/file3"), (short)3, 3);
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3);
				inFormat.SetMinSplitSizeRack(Blocksize);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_2 in splits)
				{
					System.Console.Out.WriteLine("File split(Test2): " + split_2);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(3, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// create file4 on all three racks
				Path file4 = new Path(dir4 + "/file4");
				WriteFile(conf, file4, (short)3, 3);
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				inFormat.SetMinSplitSizeRack(Blocksize);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_3 in splits)
				{
					System.Console.Out.WriteLine("File split(Test3): " + split_3);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(6, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// maximum split size is 2 blocks 
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMinSplitSizeNode(Blocksize);
				inFormat.SetMaxSplitSize(2 * Blocksize);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_4 in splits)
				{
					System.Console.Out.WriteLine("File split(Test4): " + split_4);
				}
				NUnit.Framework.Assert.AreEqual(5, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual("host3.rack3.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual("host2.rack2.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual("host1.rack1.com", fileSplit.GetLocations()[0]);
				// maximum split size is 3 blocks 
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMinSplitSizeNode(Blocksize);
				inFormat.SetMaxSplitSize(3 * Blocksize);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_5 in splits)
				{
					System.Console.Out.WriteLine("File split(Test5): " + split_5);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(3, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual("host3.rack3.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual("host2.rack2.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(3, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual("host1.rack1.com", fileSplit.GetLocations()[0]);
				// maximum split size is 4 blocks 
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMaxSplitSize(4 * Blocksize);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_6 in splits)
				{
					System.Console.Out.WriteLine("File split(Test6): " + split_6);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(4, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual("host3.rack3.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(4, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(2).GetName());
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetOffset(2));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(2));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(3).GetName());
				NUnit.Framework.Assert.AreEqual(2 * Blocksize, fileSplit.GetOffset(3));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(3));
				NUnit.Framework.Assert.AreEqual("host2.rack2.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// maximum split size is 7 blocks and min is 3 blocks
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMaxSplitSize(7 * Blocksize);
				inFormat.SetMinSplitSizeNode(3 * Blocksize);
				inFormat.SetMinSplitSizeRack(3 * Blocksize);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_7 in splits)
				{
					System.Console.Out.WriteLine("File split(Test7): " + split_7);
				}
				NUnit.Framework.Assert.AreEqual(2, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(6, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual("host3.rack3.com", fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(3, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual("host1.rack1.com", fileSplit.GetLocations()[0]);
				// Rack 1 has file1, file2 and file3 and file4
				// Rack 2 has file2 and file3 and file4
				// Rack 3 has file3 and file4
				// setup a filter so that only file1 and file2 can be combined
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.AddInputPath(job, inDir);
				inFormat.SetMinSplitSizeRack(1);
				// everything is at least rack local
				inFormat.CreatePool(new TestCombineFileInputFormat.TestFilter(dir1), new TestCombineFileInputFormat.TestFilter
					(dir2));
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_8 in splits)
				{
					System.Console.Out.WriteLine("File split(Test1): " + split_8);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(6, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				// measure performance when there are multiple pools and
				// many files in each pool.
				int numPools = 100;
				int numFiles = 1000;
				TestCombineFileInputFormat.DummyInputFormat1 inFormat1 = new TestCombineFileInputFormat.DummyInputFormat1
					(this);
				for (int i = 0; i < numFiles; i++)
				{
					FileInputFormat.SetInputPaths(job, file1);
				}
				inFormat1.SetMinSplitSizeRack(1);
				// everything is at least rack local
				Path dirNoMatch1 = new Path(inDir, "/dirxx");
				Path dirNoMatch2 = new Path(inDir, "/diryy");
				for (int i_1 = 0; i_1 < numPools; i_1++)
				{
					inFormat1.CreatePool(new TestCombineFileInputFormat.TestFilter(dirNoMatch1), new 
						TestCombineFileInputFormat.TestFilter(dirNoMatch2));
				}
				long start = Runtime.CurrentTimeMillis();
				splits = inFormat1.GetSplits(job);
				long end = Runtime.CurrentTimeMillis();
				System.Console.Out.WriteLine("Elapsed time for " + numPools + " pools " + " and "
					 + numFiles + " files is " + ((end - start) / 1000) + " seconds.");
				// This file has three whole blocks. If the maxsplit size is
				// half the block size, then there should be six splits.
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMaxSplitSize(Blocksize / 2);
				FileInputFormat.SetInputPaths(job, dir3);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_9 in splits)
				{
					System.Console.Out.WriteLine("File split(Test8): " + split_9);
				}
				NUnit.Framework.Assert.AreEqual(splits.Count, 6);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
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
			WriteDataAndSetReplication(fileSys, name, stm, replication, numBlocks);
		}

		// Creates the gzip file and return the FileStatus
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		internal static FileStatus WriteGzipFile(Configuration conf, Path name, short replication
			, int numBlocks)
		{
			FileSystem fileSys = FileSystem.Get(conf);
			GZIPOutputStream @out = new GZIPOutputStream(fileSys.Create(name, true, conf.GetInt
				("io.file.buffer.size", 4096), replication, (long)Blocksize));
			WriteDataAndSetReplication(fileSys, name, @out, replication, numBlocks);
			return fileSys.GetFileStatus(name);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		private static void WriteDataAndSetReplication(FileSystem fileSys, Path name, OutputStream
			 @out, short replication, int numBlocks)
		{
			for (int i = 0; i < numBlocks; i++)
			{
				@out.Write(databuf);
			}
			@out.Close();
			DFSTestUtil.WaitReplication(fileSys, name, replication);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeDistribution()
		{
			TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
				(this);
			int numBlocks = 60;
			long totLength = 0;
			long blockSize = 100;
			int numNodes = 10;
			long minSizeNode = 50;
			long minSizeRack = 50;
			int maxSplitSize = 200;
			// 4 blocks per split.
			string[] locations = new string[numNodes];
			for (int i = 0; i < numNodes; i++)
			{
				locations[i] = "h" + i;
			}
			string[] racks = new string[0];
			Path path = new Path("hdfs://file");
			CombineFileInputFormat.OneBlockInfo[] blocks = new CombineFileInputFormat.OneBlockInfo
				[numBlocks];
			int hostCountBase = 0;
			// Generate block list. Replication 3 per block.
			for (int i_1 = 0; i_1 < numBlocks; i_1++)
			{
				int localHostCount = hostCountBase;
				string[] blockHosts = new string[3];
				for (int j = 0; j < 3; j++)
				{
					int hostNum = localHostCount % numNodes;
					blockHosts[j] = "h" + hostNum;
					localHostCount++;
				}
				hostCountBase++;
				blocks[i_1] = new CombineFileInputFormat.OneBlockInfo(path, i_1 * blockSize, blockSize
					, blockHosts, racks);
				totLength += blockSize;
			}
			IList<InputSplit> splits = new AList<InputSplit>();
			Dictionary<string, ICollection<string>> rackToNodes = new Dictionary<string, ICollection
				<string>>();
			Dictionary<string, IList<CombineFileInputFormat.OneBlockInfo>> rackToBlocks = new 
				Dictionary<string, IList<CombineFileInputFormat.OneBlockInfo>>();
			Dictionary<CombineFileInputFormat.OneBlockInfo, string[]> blockToNodes = new Dictionary
				<CombineFileInputFormat.OneBlockInfo, string[]>();
			IDictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo>> nodeToBlocks
				 = new SortedDictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo>
				>();
			CombineFileInputFormat.OneFileInfo.PopulateBlockInfo(blocks, rackToBlocks, blockToNodes
				, nodeToBlocks, rackToNodes);
			inFormat.CreateSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength, maxSplitSize
				, minSizeNode, minSizeRack, splits);
			int expectedSplitCount = (int)(totLength / maxSplitSize);
			NUnit.Framework.Assert.AreEqual(expectedSplitCount, splits.Count);
			// Ensure 90+% of the splits have node local blocks.
			// 100% locality may not always be achieved.
			int numLocalSplits = 0;
			foreach (InputSplit inputSplit in splits)
			{
				NUnit.Framework.Assert.AreEqual(maxSplitSize, inputSplit.GetLength());
				if (inputSplit.GetLocations().Length == 1)
				{
					numLocalSplits++;
				}
			}
			NUnit.Framework.Assert.IsTrue(numLocalSplits >= 0.9 * splits.Count);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeInputSplit()
		{
			// Regression test for MAPREDUCE-4892. There are 2 nodes with all blocks on 
			// both nodes. The grouping ensures that both nodes get splits instead of 
			// just the first node
			TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
				(this);
			int numBlocks = 12;
			long totLength = 0;
			long blockSize = 100;
			long maxSize = 200;
			long minSizeNode = 50;
			long minSizeRack = 50;
			string[] locations = new string[] { "h1", "h2" };
			string[] racks = new string[0];
			Path path = new Path("hdfs://file");
			CombineFileInputFormat.OneBlockInfo[] blocks = new CombineFileInputFormat.OneBlockInfo
				[numBlocks];
			for (int i = 0; i < numBlocks; ++i)
			{
				blocks[i] = new CombineFileInputFormat.OneBlockInfo(path, i * blockSize, blockSize
					, locations, racks);
				totLength += blockSize;
			}
			IList<InputSplit> splits = new AList<InputSplit>();
			Dictionary<string, ICollection<string>> rackToNodes = new Dictionary<string, ICollection
				<string>>();
			Dictionary<string, IList<CombineFileInputFormat.OneBlockInfo>> rackToBlocks = new 
				Dictionary<string, IList<CombineFileInputFormat.OneBlockInfo>>();
			Dictionary<CombineFileInputFormat.OneBlockInfo, string[]> blockToNodes = new Dictionary
				<CombineFileInputFormat.OneBlockInfo, string[]>();
			Dictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo>> nodeToBlocks
				 = new Dictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo>>();
			CombineFileInputFormat.OneFileInfo.PopulateBlockInfo(blocks, rackToBlocks, blockToNodes
				, nodeToBlocks, rackToNodes);
			inFormat.CreateSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength, maxSize
				, minSizeNode, minSizeRack, splits);
			int expectedSplitCount = (int)(totLength / maxSize);
			NUnit.Framework.Assert.AreEqual(expectedSplitCount, splits.Count);
			HashMultiset<string> nodeSplits = HashMultiset.Create();
			for (int i_1 = 0; i_1 < expectedSplitCount; ++i_1)
			{
				InputSplit inSplit = splits[i_1];
				NUnit.Framework.Assert.AreEqual(maxSize, inSplit.GetLength());
				NUnit.Framework.Assert.AreEqual(1, inSplit.GetLocations().Length);
				nodeSplits.AddItem(inSplit.GetLocations()[0]);
			}
			NUnit.Framework.Assert.AreEqual(3, nodeSplits.Count(locations[0]));
			NUnit.Framework.Assert.AreEqual(3, nodeSplits.Count(locations[1]));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitPlacementForCompressedFiles()
		{
			MiniDFSCluster dfs = null;
			FileSystem fileSys = null;
			try
			{
				/* Start 3 datanodes, one each in rack r1, r2, r3. Create five gzipped
				*  files
				* 1) file1 and file5, just after starting the datanode on r1, with
				*    a repl factor of 1, and,
				* 2) file2, just after starting the datanode on r2, with
				*    a repl factor of 2, and,
				* 3) file3, file4 after starting the all three datanodes, with a repl
				*    factor of 3.
				* At the end, file1, file5 will be present on only datanode1, file2 will
				* be present on datanode 1 and datanode2 and
				* file3, file4 will be present on all datanodes.
				*/
				Configuration conf = new Configuration();
				conf.SetBoolean("dfs.replication.considerLoad", false);
				dfs = new MiniDFSCluster.Builder(conf).Racks(rack1).Hosts(hosts1).Build();
				dfs.WaitActive();
				fileSys = dfs.GetFileSystem();
				if (!fileSys.Mkdirs(inDir))
				{
					throw new IOException("Mkdirs failed to create " + inDir.ToString());
				}
				Path file1 = new Path(dir1 + "/file1.gz");
				FileStatus f1 = WriteGzipFile(conf, file1, (short)1, 1);
				// create another file on the same datanode
				Path file5 = new Path(dir5 + "/file5.gz");
				FileStatus f5 = WriteGzipFile(conf, file5, (short)1, 1);
				// split it using a CombinedFile input format
				TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
					(this);
				Job job = Job.GetInstance(conf);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir5);
				IList<InputSplit> splits = inFormat.GetSplits(job);
				System.Console.Out.WriteLine("Made splits(Test0): " + splits.Count);
				foreach (InputSplit split in splits)
				{
					System.Console.Out.WriteLine("File split(Test0): " + split);
				}
				NUnit.Framework.Assert.AreEqual(1, splits.Count);
				CombineFileSplit fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file5.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(f5.GetLen(), fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				dfs.StartDataNodes(conf, 1, true, null, rack2, hosts2, null);
				dfs.WaitActive();
				// create file on two datanodes.
				Path file2 = new Path(dir2 + "/file2.gz");
				FileStatus f2 = WriteGzipFile(conf, file2, (short)2, 2);
				// split it using a CombinedFile input format
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2);
				inFormat.SetMinSplitSizeRack(f1.GetLen());
				splits = inFormat.GetSplits(job);
				System.Console.Out.WriteLine("Made splits(Test1): " + splits.Count);
				// make sure that each split has different locations
				foreach (InputSplit split_1 in splits)
				{
					System.Console.Out.WriteLine("File split(Test1): " + split_1);
				}
				NUnit.Framework.Assert.AreEqual(2, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f2.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// create another file on 3 datanodes and 3 racks.
				dfs.StartDataNodes(conf, 1, true, null, rack3, hosts3, null);
				dfs.WaitActive();
				Path file3 = new Path(dir3 + "/file3.gz");
				FileStatus f3 = WriteGzipFile(conf, file3, (short)3, 3);
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3);
				inFormat.SetMinSplitSizeRack(f1.GetLen());
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_2 in splits)
				{
					System.Console.Out.WriteLine("File split(Test2): " + split_2);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f3.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f2.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// create file4 on all three racks
				Path file4 = new Path(dir4 + "/file4.gz");
				FileStatus f4 = WriteGzipFile(conf, file4, (short)3, 3);
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				inFormat.SetMinSplitSizeRack(f1.GetLen());
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_3 in splits)
				{
					System.Console.Out.WriteLine("File split(Test3): " + split_3);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f3.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(f4.GetLen(), fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f2.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// maximum split size is file1's length
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMinSplitSizeNode(f1.GetLen());
				inFormat.SetMaxSplitSize(f1.GetLen());
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_4 in splits)
				{
					System.Console.Out.WriteLine("File split(Test4): " + split_4);
				}
				NUnit.Framework.Assert.AreEqual(4, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f3.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f2.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r3
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[3];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f4.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// maximum split size is twice file1's length
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMinSplitSizeNode(f1.GetLen());
				inFormat.SetMaxSplitSize(2 * f1.GetLen());
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_5 in splits)
				{
					System.Console.Out.WriteLine("File split(Test5): " + split_5);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f3.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(f4.GetLen(), fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f2.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// maximum split size is 4 times file1's length 
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMinSplitSizeNode(2 * f1.GetLen());
				inFormat.SetMaxSplitSize(4 * f1.GetLen());
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_6 in splits)
				{
					System.Console.Out.WriteLine("File split(Test6): " + split_6);
				}
				NUnit.Framework.Assert.AreEqual(2, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file3.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f3.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file4.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(f4.GetLen(), fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(f1.GetLen(), fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file2.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1), Blocksize);
				NUnit.Framework.Assert.AreEqual(f2.GetLen(), fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				// maximum split size and min-split-size per rack is 4 times file1's length
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMaxSplitSize(4 * f1.GetLen());
				inFormat.SetMinSplitSizeRack(4 * f1.GetLen());
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_7 in splits)
				{
					System.Console.Out.WriteLine("File split(Test7): " + split_7);
				}
				NUnit.Framework.Assert.AreEqual(1, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(4, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// minimum split size per node is 4 times file1's length
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				inFormat.SetMinSplitSizeNode(4 * f1.GetLen());
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir2 + "," + dir3 + "," + dir4);
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_8 in splits)
				{
					System.Console.Out.WriteLine("File split(Test8): " + split_8);
				}
				NUnit.Framework.Assert.AreEqual(1, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(4, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// Rack 1 has file1, file2 and file3 and file4
				// Rack 2 has file2 and file3 and file4
				// Rack 3 has file3 and file4
				// setup a filter so that only file1 and file2 can be combined
				inFormat = new TestCombineFileInputFormat.DummyInputFormat(this);
				FileInputFormat.AddInputPath(job, inDir);
				inFormat.SetMinSplitSizeRack(1);
				// everything is at least rack local
				inFormat.CreatePool(new TestCombineFileInputFormat.TestFilter(dir1), new TestCombineFileInputFormat.TestFilter
					(dir2));
				splits = inFormat.GetSplits(job);
				foreach (InputSplit split_9 in splits)
				{
					System.Console.Out.WriteLine("File split(Test9): " + split_9);
				}
				NUnit.Framework.Assert.AreEqual(3, splits.Count);
				fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts2[0], fileSplit.GetLocations()[0]);
				// should be on r2
				fileSplit = (CombineFileSplit)splits[1];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
				// should be on r1
				fileSplit = (CombineFileSplit)splits[2];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(hosts3[0], fileSplit.GetLocations()[0]);
				// should be on r3
				// measure performance when there are multiple pools and
				// many files in each pool.
				int numPools = 100;
				int numFiles = 1000;
				TestCombineFileInputFormat.DummyInputFormat1 inFormat1 = new TestCombineFileInputFormat.DummyInputFormat1
					(this);
				for (int i = 0; i < numFiles; i++)
				{
					FileInputFormat.SetInputPaths(job, file1);
				}
				inFormat1.SetMinSplitSizeRack(1);
				// everything is at least rack local
				Path dirNoMatch1 = new Path(inDir, "/dirxx");
				Path dirNoMatch2 = new Path(inDir, "/diryy");
				for (int i_1 = 0; i_1 < numPools; i_1++)
				{
					inFormat1.CreatePool(new TestCombineFileInputFormat.TestFilter(dirNoMatch1), new 
						TestCombineFileInputFormat.TestFilter(dirNoMatch2));
				}
				long start = Runtime.CurrentTimeMillis();
				splits = inFormat1.GetSplits(job);
				long end = Runtime.CurrentTimeMillis();
				System.Console.Out.WriteLine("Elapsed time for " + numPools + " pools " + " and "
					 + numFiles + " files is " + ((end - start)) + " milli seconds.");
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
		}

		/// <summary>Test that CFIF can handle missing blocks.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingBlocks()
		{
			string namenode = null;
			MiniDFSCluster dfs = null;
			FileSystem fileSys = null;
			string testName = "testMissingBlocks";
			try
			{
				Configuration conf = new Configuration();
				conf.Set("fs.hdfs.impl", typeof(TestCombineFileInputFormat.MissingBlockFileSystem
					).FullName);
				conf.SetBoolean("dfs.replication.considerLoad", false);
				dfs = new MiniDFSCluster.Builder(conf).Racks(rack1).Hosts(hosts1).Build();
				dfs.WaitActive();
				namenode = (dfs.GetFileSystem()).GetUri().GetHost() + ":" + (dfs.GetFileSystem())
					.GetUri().GetPort();
				fileSys = dfs.GetFileSystem();
				if (!fileSys.Mkdirs(inDir))
				{
					throw new IOException("Mkdirs failed to create " + inDir.ToString());
				}
				Path file1 = new Path(dir1 + "/file1");
				WriteFile(conf, file1, (short)1, 1);
				// create another file on the same datanode
				Path file5 = new Path(dir5 + "/file5");
				WriteFile(conf, file5, (short)1, 1);
				((TestCombineFileInputFormat.MissingBlockFileSystem)fileSys).SetFileWithMissingBlocks
					(file1.ToUri().GetPath());
				// split it using a CombinedFile input format
				TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
					(this);
				Job job = Job.GetInstance(conf);
				FileInputFormat.SetInputPaths(job, dir1 + "," + dir5);
				IList<InputSplit> splits = inFormat.GetSplits(job);
				System.Console.Out.WriteLine("Made splits(Test0): " + splits.Count);
				foreach (InputSplit split in splits)
				{
					System.Console.Out.WriteLine("File split(Test0): " + split);
				}
				NUnit.Framework.Assert.AreEqual(splits.Count, 1);
				CombineFileSplit fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(2, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetLocations().Length);
				NUnit.Framework.Assert.AreEqual(file1.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(0));
				NUnit.Framework.Assert.AreEqual(file5.GetName(), fileSplit.GetPath(1).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(1));
				NUnit.Framework.Assert.AreEqual(Blocksize, fileSplit.GetLength(1));
				NUnit.Framework.Assert.AreEqual(hosts1[0], fileSplit.GetLocations()[0]);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
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
				, 4096), (short)1, (long)Blocksize);
			@out.Write(new byte[0]);
			@out.Close();
			// split it using a CombinedFile input format
			TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
				(this);
			Job job = Job.GetInstance(conf);
			FileInputFormat.SetInputPaths(job, "test");
			IList<InputSplit> splits = inFormat.GetSplits(job);
			NUnit.Framework.Assert.AreEqual(1, splits.Count);
			CombineFileSplit fileSplit = (CombineFileSplit)splits[0];
			NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
			NUnit.Framework.Assert.AreEqual(file.GetName(), fileSplit.GetPath(0).GetName());
			NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
			NUnit.Framework.Assert.AreEqual(0, fileSplit.GetLength(0));
			fileSys.Delete(file.GetParent(), true);
		}

		/// <summary>Test that directories do not get included as part of getSplits()</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSplitsWithDirectory()
		{
			MiniDFSCluster dfs = null;
			try
			{
				Configuration conf = new Configuration();
				dfs = new MiniDFSCluster.Builder(conf).Racks(rack1).Hosts(hosts1).Build();
				dfs.WaitActive();
				FileSystem fileSys = dfs.GetFileSystem();
				// Set up the following directory structure:
				// /dir1/: directory
				// /dir1/file: regular file
				// /dir1/dir2/: directory
				Path dir1 = new Path("/dir1");
				Path file = new Path("/dir1/file1");
				Path dir2 = new Path("/dir1/dir2");
				if (!fileSys.Mkdirs(dir1))
				{
					throw new IOException("Mkdirs failed to create " + dir1.ToString());
				}
				FSDataOutputStream @out = fileSys.Create(file);
				@out.Write(new byte[0]);
				@out.Close();
				if (!fileSys.Mkdirs(dir2))
				{
					throw new IOException("Mkdirs failed to create " + dir2.ToString());
				}
				// split it using a CombinedFile input format
				TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
					(this);
				Job job = Job.GetInstance(conf);
				FileInputFormat.SetInputPaths(job, "/dir1");
				IList<InputSplit> splits = inFormat.GetSplits(job);
				// directories should be omitted from getSplits() - we should only see file1 and not dir2
				NUnit.Framework.Assert.AreEqual(1, splits.Count);
				CombineFileSplit fileSplit = (CombineFileSplit)splits[0];
				NUnit.Framework.Assert.AreEqual(1, fileSplit.GetNumPaths());
				NUnit.Framework.Assert.AreEqual(file.GetName(), fileSplit.GetPath(0).GetName());
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetOffset(0));
				NUnit.Framework.Assert.AreEqual(0, fileSplit.GetLength(0));
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
		}

		/// <summary>Test when input files are from non-default file systems</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestForNonDefaultFileSystem()
		{
			Configuration conf = new Configuration();
			// use a fake file system scheme as default
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, DummyFsUri);
			// default fs path
			NUnit.Framework.Assert.AreEqual(DummyFsUri, FileSystem.GetDefaultUri(conf).ToString
				());
			// add a local file
			Path localPath = new Path("testFile1");
			FileSystem lfs = FileSystem.GetLocal(conf);
			FSDataOutputStream dos = lfs.Create(localPath);
			dos.WriteChars("Local file for CFIF");
			dos.Close();
			Job job = Job.GetInstance(conf);
			FileInputFormat.SetInputPaths(job, lfs.MakeQualified(localPath));
			TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
				(this);
			IList<InputSplit> splits = inFormat.GetSplits(job);
			NUnit.Framework.Assert.IsTrue(splits.Count > 0);
			foreach (InputSplit s in splits)
			{
				CombineFileSplit cfs = (CombineFileSplit)s;
				foreach (Path p in cfs.GetPaths())
				{
					NUnit.Framework.Assert.AreEqual(p.ToUri().GetScheme(), "file");
				}
			}
		}

		internal class TestFilter : PathFilter
		{
			private Path p;

			public TestFilter(Path p)
			{
				// store a path prefix in this TestFilter
				this.p = p;
			}

			// returns true if the specified path matches the prefix stored
			// in this TestFilter.
			public virtual bool Accept(Path path)
			{
				if (path.ToUri().GetPath().IndexOf(p.ToString()) == 0)
				{
					return true;
				}
				return false;
			}

			public override string ToString()
			{
				return "PathFilter:" + p;
			}
		}

		/*
		* Prints out the input splits for the specified files
		*/
		/// <exception cref="System.IO.IOException"/>
		private void SplitRealFiles(string[] args)
		{
			Configuration conf = new Configuration();
			Job job = Job.GetInstance();
			FileSystem fs = FileSystem.Get(conf);
			if (!(fs is DistributedFileSystem))
			{
				throw new IOException("Wrong file system: " + fs.GetType().FullName);
			}
			long blockSize = fs.GetDefaultBlockSize();
			TestCombineFileInputFormat.DummyInputFormat inFormat = new TestCombineFileInputFormat.DummyInputFormat
				(this);
			for (int i = 0; i < args.Length; i++)
			{
				FileInputFormat.AddInputPaths(job, args[i]);
			}
			inFormat.SetMinSplitSizeRack(blockSize);
			inFormat.SetMaxSplitSize(10 * blockSize);
			IList<InputSplit> splits = inFormat.GetSplits(job);
			System.Console.Out.WriteLine("Total number of splits " + splits.Count);
			for (int i_1 = 0; i_1 < splits.Count; ++i_1)
			{
				CombineFileSplit fileSplit = (CombineFileSplit)splits[i_1];
				System.Console.Out.WriteLine("Split[" + i_1 + "] " + fileSplit);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			// if there are some parameters specified, then use those paths
			if (args.Length != 0)
			{
				TestCombineFileInputFormat test = new TestCombineFileInputFormat();
				test.SplitRealFiles(args);
			}
			else
			{
				TestCombineFileInputFormat test = new TestCombineFileInputFormat();
				test.TestSplitPlacement();
			}
		}

		public TestCombineFileInputFormat()
		{
			dir1 = new Path(inDir, "/dir1");
			dir2 = new Path(inDir, "/dir2");
			dir3 = new Path(inDir, "/dir3");
			dir4 = new Path(inDir, "/dir4");
			dir5 = new Path(inDir, "/dir5");
		}
	}
}
