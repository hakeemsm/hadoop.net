using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Junit 4 test for slive</summary>
	public class TestSlive
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSlive));

		private static readonly Random rnd = new Random(1L);

		private const string TestDataProp = "test.build.data";

		private static Configuration GetBaseConfig()
		{
			Configuration conf = new Configuration();
			return conf;
		}

		/// <summary>gets the test write location according to the coding guidelines</summary>
		private static FilePath GetWriteLoc()
		{
			string writeLoc = Runtime.GetProperty(TestDataProp, "build/test/data/");
			FilePath writeDir = new FilePath(writeLoc, "slive");
			writeDir.Mkdirs();
			return writeDir;
		}

		/// <summary>gets where the MR job places its data + output + results</summary>
		private static FilePath GetFlowLocation()
		{
			return new FilePath(GetWriteLoc(), "flow");
		}

		/// <summary>gets the test directory which is created by the mkdir op</summary>
		private static FilePath GetTestDir()
		{
			return new FilePath(GetWriteLoc(), "slivedir");
		}

		/// <summary>
		/// gets the test file location
		/// which is used for reading, appending and created
		/// </summary>
		private static FilePath GetTestFile()
		{
			return new FilePath(GetWriteLoc(), "slivefile");
		}

		/// <summary>
		/// gets the rename file which is used in combination
		/// with the test file to do a rename operation
		/// </summary>
		private static FilePath GetTestRenameFile()
		{
			return new FilePath(GetWriteLoc(), "slivefile1");
		}

		/// <summary>gets the MR result file name</summary>
		private static FilePath GetResultFile()
		{
			return new FilePath(GetWriteLoc(), "sliveresfile");
		}

		private static FilePath GetImaginaryFile()
		{
			return new FilePath(GetWriteLoc(), "slivenofile");
		}

		/// <summary>gets the test program arguments used for merging and main MR running</summary>
		private string[] GetTestArgs(bool sleep)
		{
			IList<string> args = new List<string>();
			{
				// setup the options
				args.AddItem("-" + ConfigOption.WriteSize.GetOpt());
				args.AddItem("1M,2M");
				args.AddItem("-" + ConfigOption.Ops.GetOpt());
				args.AddItem(Constants.OperationType.Values().Length + string.Empty);
				args.AddItem("-" + ConfigOption.Maps.GetOpt());
				args.AddItem("2");
				args.AddItem("-" + ConfigOption.Reduces.GetOpt());
				args.AddItem("2");
				args.AddItem("-" + ConfigOption.AppendSize.GetOpt());
				args.AddItem("1M,2M");
				args.AddItem("-" + ConfigOption.BlockSize.GetOpt());
				args.AddItem("1M,2M");
				args.AddItem("-" + ConfigOption.ReplicationAm.GetOpt());
				args.AddItem("1,1");
				if (sleep)
				{
					args.AddItem("-" + ConfigOption.SleepTime.GetOpt());
					args.AddItem("10,10");
				}
				args.AddItem("-" + ConfigOption.ResultFile.GetOpt());
				args.AddItem(GetResultFile().ToString());
				args.AddItem("-" + ConfigOption.BaseDir.GetOpt());
				args.AddItem(GetFlowLocation().ToString());
				args.AddItem("-" + ConfigOption.Duration.GetOpt());
				args.AddItem("10");
				args.AddItem("-" + ConfigOption.DirSize.GetOpt());
				args.AddItem("10");
				args.AddItem("-" + ConfigOption.Files.GetOpt());
				args.AddItem("10");
				args.AddItem("-" + ConfigOption.TruncateSize.GetOpt());
				args.AddItem("0,1M");
			}
			return Sharpen.Collections.ToArray(args, new string[args.Count]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFinder()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			PathFinder fr = new PathFinder(extractor, rnd);
			// should only be able to select 10 files
			// attempt for a given amount of iterations
			int maxIterations = 10000;
			ICollection<Path> files = new HashSet<Path>();
			for (int i = 0; i < maxIterations; i++)
			{
				files.AddItem(fr.GetFile());
			}
			NUnit.Framework.Assert.IsTrue(files.Count == 10);
			ICollection<Path> dirs = new HashSet<Path>();
			for (int i_1 = 0; i_1 < maxIterations; i_1++)
			{
				dirs.AddItem(fr.GetDirectory());
			}
			NUnit.Framework.Assert.IsTrue(dirs.Count == 10);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSelection()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			WeightSelector selector = new WeightSelector(extractor, rnd);
			// should be 1 of each type - uniform
			int expected = Constants.OperationType.Values().Length;
			Operation op = null;
			ICollection<string> types = new HashSet<string>();
			FileSystem fs = FileSystem.Get(extractor.GetConfig());
			while (true)
			{
				op = selector.Select(1, 1);
				if (op == null)
				{
					break;
				}
				// doesn't matter if they work or not
				op.Run(fs);
				types.AddItem(op.GetType());
			}
			NUnit.Framework.Assert.AreEqual(types.Count, expected);
		}

		// gets the config merged with the arguments
		/// <exception cref="System.Exception"/>
		private ConfigExtractor GetTestConfig(bool sleep)
		{
			ArgumentParser parser = new ArgumentParser(GetTestArgs(sleep));
			ArgumentParser.ParsedOutput @out = parser.Parse();
			NUnit.Framework.Assert.IsTrue(!@out.ShouldOutputHelp());
			ConfigMerger merge = new ConfigMerger();
			Configuration cfg = merge.GetMerged(@out, GetBaseConfig());
			ConfigExtractor extractor = new ConfigExtractor(cfg);
			return extractor;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void EnsureDeleted()
		{
			RDelete(GetTestFile());
			RDelete(GetTestDir());
			RDelete(GetTestRenameFile());
			RDelete(GetResultFile());
			RDelete(GetFlowLocation());
			RDelete(GetImaginaryFile());
		}

		/// <summary>cleans up a file or directory recursively if need be</summary>
		/// <exception cref="System.Exception"/>
		private void RDelete(FilePath place)
		{
			if (place.IsFile())
			{
				Log.Info("Deleting file " + place);
				NUnit.Framework.Assert.IsTrue(place.Delete());
			}
			else
			{
				if (place.IsDirectory())
				{
					DeleteDir(place);
				}
			}
		}

		/// <summary>deletes a dir and its contents</summary>
		/// <exception cref="System.Exception"/>
		private void DeleteDir(FilePath dir)
		{
			string[] fns = dir.List();
			// delete contents first
			foreach (string afn in fns)
			{
				FilePath fn = new FilePath(dir, afn);
				RDelete(fn);
			}
			Log.Info("Deleting directory " + dir);
			// now delete the dir
			NUnit.Framework.Assert.IsTrue(dir.Delete());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestArguments()
		{
			ConfigExtractor extractor = GetTestConfig(true);
			NUnit.Framework.Assert.AreEqual(extractor.GetOpCount(), Constants.OperationType.Values
				().Length);
			NUnit.Framework.Assert.AreEqual(extractor.GetMapAmount(), 2);
			NUnit.Framework.Assert.AreEqual(extractor.GetReducerAmount(), 2);
			Range<long> apRange = extractor.GetAppendSize();
			NUnit.Framework.Assert.AreEqual(apRange.GetLower(), Constants.Megabytes * 1);
			NUnit.Framework.Assert.AreEqual(apRange.GetUpper(), Constants.Megabytes * 2);
			Range<long> wRange = extractor.GetWriteSize();
			NUnit.Framework.Assert.AreEqual(wRange.GetLower(), Constants.Megabytes * 1);
			NUnit.Framework.Assert.AreEqual(wRange.GetUpper(), Constants.Megabytes * 2);
			Range<long> trRange = extractor.GetTruncateSize();
			NUnit.Framework.Assert.AreEqual(trRange.GetLower(), 0);
			NUnit.Framework.Assert.AreEqual(trRange.GetUpper(), Constants.Megabytes * 1);
			Range<long> bRange = extractor.GetBlockSize();
			NUnit.Framework.Assert.AreEqual(bRange.GetLower(), Constants.Megabytes * 1);
			NUnit.Framework.Assert.AreEqual(bRange.GetUpper(), Constants.Megabytes * 2);
			string resfile = extractor.GetResultFile();
			NUnit.Framework.Assert.AreEqual(resfile, GetResultFile().ToString());
			int durationMs = extractor.GetDurationMilliseconds();
			NUnit.Framework.Assert.AreEqual(durationMs, 10 * 1000);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataWriting()
		{
			long byteAm = 100;
			FilePath fn = GetTestFile();
			DataWriter writer = new DataWriter(rnd);
			FileOutputStream fs = new FileOutputStream(fn);
			DataWriter.GenerateOutput ostat = writer.WriteSegment(byteAm, fs);
			Log.Info(ostat);
			fs.Close();
			NUnit.Framework.Assert.IsTrue(ostat.GetBytesWritten() == byteAm);
			DataVerifier vf = new DataVerifier();
			FileInputStream fin = new FileInputStream(fn);
			DataVerifier.VerifyOutput vfout = vf.VerifyFile(byteAm, new DataInputStream(fin));
			Log.Info(vfout);
			fin.Close();
			NUnit.Framework.Assert.AreEqual(vfout.GetBytesRead(), byteAm);
			NUnit.Framework.Assert.IsTrue(vfout.GetChunksDifferent() == 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestRange()
		{
			Range<long> r = new Range<long>(10L, 20L);
			NUnit.Framework.Assert.AreEqual(r.GetLower(), 10L);
			NUnit.Framework.Assert.AreEqual(r.GetUpper(), 20L);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateOp()
		{
			// setup a valid config
			ConfigExtractor extractor = GetTestConfig(false);
			Path fn = new Path(GetTestFile().GetCanonicalPath());
			CreateOp op = new _CreateOp_285(fn, extractor, rnd);
			RunOperationOk(extractor, op, true);
		}

		private sealed class _CreateOp_285 : CreateOp
		{
			public _CreateOp_285(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetCreateFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOpFailures()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			Path fn = new Path(GetImaginaryFile().GetCanonicalPath());
			ReadOp rop = new _ReadOp_297(fn, extractor, rnd);
			RunOperationBad(extractor, rop);
			DeleteOp dop = new _DeleteOp_304(fn, extractor, rnd);
			RunOperationBad(extractor, dop);
			RenameOp reop = new _RenameOp_311(fn, extractor, rnd);
			RunOperationBad(extractor, reop);
			AppendOp aop = new _AppendOp_318(fn, extractor, rnd);
			RunOperationBad(extractor, aop);
		}

		private sealed class _ReadOp_297 : ReadOp
		{
			public _ReadOp_297(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetReadFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		private sealed class _DeleteOp_304 : DeleteOp
		{
			public _DeleteOp_304(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetDeleteFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		private sealed class _RenameOp_311 : RenameOp
		{
			public _RenameOp_311(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override RenameOp.SrcTarget GetRenames()
			{
				return new RenameOp.SrcTarget(fn, fn);
			}

			private readonly Path fn;
		}

		private sealed class _AppendOp_318 : AppendOp
		{
			public _AppendOp_318(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetAppendFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		/// <exception cref="System.Exception"/>
		private void RunOperationBad(ConfigExtractor cfg, Operation op)
		{
			FileSystem fs = FileSystem.Get(cfg.GetConfig());
			IList<OperationOutput> data = op.Run(fs);
			NUnit.Framework.Assert.IsTrue(!data.IsEmpty());
			bool foundFail = false;
			foreach (OperationOutput d in data)
			{
				if (d.GetMeasurementType().Equals(ReportWriter.Failures))
				{
					foundFail = true;
				}
				if (d.GetMeasurementType().Equals(ReportWriter.NotFound))
				{
					foundFail = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(foundFail);
		}

		/// <exception cref="System.Exception"/>
		private void RunOperationOk(ConfigExtractor cfg, Operation op, bool checkOk)
		{
			FileSystem fs = FileSystem.Get(cfg.GetConfig());
			IList<OperationOutput> data = op.Run(fs);
			NUnit.Framework.Assert.IsTrue(!data.IsEmpty());
			if (checkOk)
			{
				bool foundSuc = false;
				bool foundOpCount = false;
				bool foundTime = false;
				foreach (OperationOutput d in data)
				{
					NUnit.Framework.Assert.IsTrue(!d.GetMeasurementType().Equals(ReportWriter.Failures
						));
					if (d.GetMeasurementType().Equals(ReportWriter.Successes))
					{
						foundSuc = true;
					}
					if (d.GetMeasurementType().Equals(ReportWriter.OpCount))
					{
						foundOpCount = true;
					}
					if (d.GetMeasurementType().Equals(ReportWriter.OkTimeTaken))
					{
						foundTime = true;
					}
				}
				NUnit.Framework.Assert.IsTrue(foundSuc);
				NUnit.Framework.Assert.IsTrue(foundOpCount);
				NUnit.Framework.Assert.IsTrue(foundTime);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelete()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			Path fn = new Path(GetTestFile().GetCanonicalPath());
			// ensure file created before delete
			CreateOp op = new _CreateOp_376(fn, extractor, rnd);
			RunOperationOk(extractor, op, true);
			// now delete
			DeleteOp dop = new _DeleteOp_383(fn, extractor, rnd);
			RunOperationOk(extractor, dop, true);
		}

		private sealed class _CreateOp_376 : CreateOp
		{
			public _CreateOp_376(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetCreateFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		private sealed class _DeleteOp_383 : DeleteOp
		{
			public _DeleteOp_383(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetDeleteFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			Path src = new Path(GetTestFile().GetCanonicalPath());
			Path tgt = new Path(GetTestRenameFile().GetCanonicalPath());
			// ensure file created before rename
			CreateOp op = new _CreateOp_397(src, extractor, rnd);
			RunOperationOk(extractor, op, true);
			RenameOp rop = new _RenameOp_403(src, tgt, extractor, rnd);
			RunOperationOk(extractor, rop, true);
		}

		private sealed class _CreateOp_397 : CreateOp
		{
			public _CreateOp_397(Path src, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.src = src;
			}

			protected internal override Path GetCreateFile()
			{
				return src;
			}

			private readonly Path src;
		}

		private sealed class _RenameOp_403 : RenameOp
		{
			public _RenameOp_403(Path src, Path tgt, ConfigExtractor baseArg1, Random baseArg2
				)
				: base(baseArg1, baseArg2)
			{
				this.src = src;
				this.tgt = tgt;
			}

			protected internal override RenameOp.SrcTarget GetRenames()
			{
				return new RenameOp.SrcTarget(src, tgt);
			}

			private readonly Path src;

			private readonly Path tgt;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRFlow()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			SliveTest s = new SliveTest(GetBaseConfig());
			int ec = ToolRunner.Run(s, GetTestArgs(false));
			NUnit.Framework.Assert.IsTrue(ec == 0);
			string resFile = extractor.GetResultFile();
			FilePath fn = new FilePath(resFile);
			NUnit.Framework.Assert.IsTrue(fn.Exists());
		}

		// can't validate completely since operations may fail (mainly anyone but
		// create +mkdir) since they may not find there files
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRead()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			Path fn = new Path(GetTestFile().GetCanonicalPath());
			// ensure file created before read
			CreateOp op = new _CreateOp_429(fn, extractor, rnd);
			RunOperationOk(extractor, op, true);
			ReadOp rop = new _ReadOp_435(fn, extractor, rnd);
			RunOperationOk(extractor, rop, true);
		}

		private sealed class _CreateOp_429 : CreateOp
		{
			public _CreateOp_429(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetCreateFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		private sealed class _ReadOp_435 : ReadOp
		{
			public _ReadOp_435(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetReadFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSleep()
		{
			ConfigExtractor extractor = GetTestConfig(true);
			SleepOp op = new SleepOp(extractor, rnd);
			RunOperationOk(extractor, op, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestList()
		{
			// ensure dir made
			ConfigExtractor extractor = GetTestConfig(false);
			Path dir = new Path(GetTestDir().GetCanonicalPath());
			MkdirOp op = new _MkdirOp_455(dir, extractor, rnd);
			RunOperationOk(extractor, op, true);
			// list it
			ListOp lop = new _ListOp_462(dir, extractor, rnd);
			RunOperationOk(extractor, lop, true);
		}

		private sealed class _MkdirOp_455 : MkdirOp
		{
			public _MkdirOp_455(Path dir, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dir = dir;
			}

			protected internal override Path GetDirectory()
			{
				return dir;
			}

			private readonly Path dir;
		}

		private sealed class _ListOp_462 : ListOp
		{
			public _ListOp_462(Path dir, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dir = dir;
			}

			protected internal override Path GetDirectory()
			{
				return dir;
			}

			private readonly Path dir;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadChunks()
		{
			FilePath fn = GetTestFile();
			int byteAm = 10000;
			FileOutputStream fout = new FileOutputStream(fn);
			byte[] bytes = new byte[byteAm];
			rnd.NextBytes(bytes);
			fout.Write(bytes);
			fout.Close();
			// attempt to read it
			DataVerifier vf = new DataVerifier();
			DataVerifier.VerifyOutput vout = new DataVerifier.VerifyOutput(0, 0, 0, 0);
			DataInputStream @in = null;
			try
			{
				@in = new DataInputStream(new FileInputStream(fn));
				vout = vf.VerifyFile(byteAm, @in);
			}
			catch (Exception)
			{
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
			NUnit.Framework.Assert.IsTrue(vout.GetChunksSame() == 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdir()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			Path dir = new Path(GetTestDir().GetCanonicalPath());
			MkdirOp op = new _MkdirOp_498(dir, extractor, rnd);
			RunOperationOk(extractor, op, true);
		}

		private sealed class _MkdirOp_498 : MkdirOp
		{
			public _MkdirOp_498(Path dir, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dir = dir;
			}

			protected internal override Path GetDirectory()
			{
				return dir;
			}

			private readonly Path dir;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSelector()
		{
			ConfigExtractor extractor = GetTestConfig(false);
			RouletteSelector selector = new RouletteSelector(rnd);
			IList<OperationWeight> sList = new List<OperationWeight>();
			Operation op = selector.Select(sList);
			NUnit.Framework.Assert.IsTrue(op == null);
			CreateOp cop = new CreateOp(extractor, rnd);
			sList.AddItem(new OperationWeight(cop, 1.0d));
			AppendOp aop = new AppendOp(extractor, rnd);
			sList.AddItem(new OperationWeight(aop, 0.01d));
			op = selector.Select(sList);
			NUnit.Framework.Assert.IsTrue(op == cop);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendOp()
		{
			// setup a valid config
			ConfigExtractor extractor = GetTestConfig(false);
			// ensure file created before append
			Path fn = new Path(GetTestFile().GetCanonicalPath());
			CreateOp op = new _CreateOp_527(fn, extractor, rnd);
			RunOperationOk(extractor, op, true);
			// local file system (ChecksumFileSystem) currently doesn't support append -
			// but we'll leave this test here anyways but can't check the results..
			AppendOp aop = new _AppendOp_535(fn, extractor, rnd);
			RunOperationOk(extractor, aop, false);
		}

		private sealed class _CreateOp_527 : CreateOp
		{
			public _CreateOp_527(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetCreateFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		private sealed class _AppendOp_535 : AppendOp
		{
			public _AppendOp_535(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetAppendFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateOp()
		{
			// setup a valid config
			ConfigExtractor extractor = GetTestConfig(false);
			// ensure file created before append
			Path fn = new Path(GetTestFile().GetCanonicalPath());
			CreateOp op = new _CreateOp_549(fn, extractor, rnd);
			RunOperationOk(extractor, op, true);
			// local file system (ChecksumFileSystem) currently doesn't support truncate -
			// but we'll leave this test here anyways but can't check the results..
			TruncateOp top = new _TruncateOp_557(fn, extractor, rnd);
			RunOperationOk(extractor, top, false);
		}

		private sealed class _CreateOp_549 : CreateOp
		{
			public _CreateOp_549(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetCreateFile()
			{
				return fn;
			}

			private readonly Path fn;
		}

		private sealed class _TruncateOp_557 : TruncateOp
		{
			public _TruncateOp_557(Path fn, ConfigExtractor baseArg1, Random baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.fn = fn;
			}

			protected internal override Path GetTruncateFile()
			{
				return fn;
			}

			private readonly Path fn;
		}
	}
}
