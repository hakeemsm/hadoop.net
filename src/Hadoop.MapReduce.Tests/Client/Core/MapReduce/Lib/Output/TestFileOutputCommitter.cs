using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	public class TestFileOutputCommitter : TestCase
	{
		private static readonly Path outDir = new Path(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), typeof(TestFileOutputCommitter).FullName
			);

		private const string SubDir = "SUB_DIR";

		private static readonly Path OutSubDir = new Path(outDir, SubDir);

		private static readonly Log Log = LogFactory.GetLog(typeof(TestFileOutputCommitter
			));

		private const string attempt = "attempt_200707121733_0001_m_000000_0";

		private const string partFile = "part-m-00000";

		private static readonly TaskAttemptID taskID = TaskAttemptID.ForName(attempt);

		private const string attempt1 = "attempt_200707121733_0001_m_000001_0";

		private static readonly TaskAttemptID taskID1 = TaskAttemptID.ForName(attempt1);

		private Text key1 = new Text("key1");

		private Text key2 = new Text("key2");

		private Text val1 = new Text("val1");

		private Text val2 = new Text("val2");

		// A random task attempt id for testing.
		/// <exception cref="System.IO.IOException"/>
		private static void Cleanup()
		{
			Configuration conf = new Configuration();
			FileSystem fs = outDir.GetFileSystem(conf);
			fs.Delete(outDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			Cleanup();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			Cleanup();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void WriteOutput(RecordWriter theRecordWriter, TaskAttemptContext context
			)
		{
			NullWritable nullWritable = NullWritable.Get();
			try
			{
				theRecordWriter.Write(key1, val1);
				theRecordWriter.Write(null, nullWritable);
				theRecordWriter.Write(null, val1);
				theRecordWriter.Write(nullWritable, val2);
				theRecordWriter.Write(key2, nullWritable);
				theRecordWriter.Write(key1, null);
				theRecordWriter.Write(null, null);
				theRecordWriter.Write(key2, val2);
			}
			finally
			{
				theRecordWriter.Close(context);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void WriteMapFileOutput(RecordWriter theRecordWriter, TaskAttemptContext 
			context)
		{
			try
			{
				int key = 0;
				for (int i = 0; i < 10; ++i)
				{
					key = i;
					Text val = (i % 2 == 1) ? val1 : val2;
					theRecordWriter.Write(new LongWritable(key), val);
				}
			}
			finally
			{
				theRecordWriter.Close(context);
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestRecoveryInternal(int commitVersion, int recoveryVersion)
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(MRJobConfig.ApplicationAttemptId, 1);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, commitVersion
				);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			WriteOutput(theRecordWriter, tContext);
			// do commit
			committer.CommitTask(tContext);
			Path jobTempDir1 = committer.GetCommittedTaskPath(tContext);
			FilePath jtd = new FilePath(jobTempDir1.ToUri().GetPath());
			if (commitVersion == 1)
			{
				NUnit.Framework.Assert.IsTrue("Version 1 commits to temporary dir " + jtd, jtd.Exists
					());
				ValidateContent(jtd);
			}
			else
			{
				NUnit.Framework.Assert.IsFalse("Version 2 commits to output dir " + jtd, jtd.Exists
					());
			}
			//now while running the second app attempt, 
			//recover the task output from first attempt
			Configuration conf2 = job.GetConfiguration();
			conf2.Set(MRJobConfig.TaskAttemptId, attempt);
			conf2.SetInt(MRJobConfig.ApplicationAttemptId, 2);
			conf2.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, recoveryVersion
				);
			JobContext jContext2 = new JobContextImpl(conf2, taskID.GetJobID());
			TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2, taskID);
			FileOutputCommitter committer2 = new FileOutputCommitter(outDir, tContext2);
			committer2.SetupJob(tContext2);
			Path jobTempDir2 = committer2.GetCommittedTaskPath(tContext2);
			FilePath jtd2 = new FilePath(jobTempDir2.ToUri().GetPath());
			committer2.RecoverTask(tContext2);
			if (recoveryVersion == 1)
			{
				NUnit.Framework.Assert.IsTrue("Version 1 recovers to " + jtd2, jtd2.Exists());
				ValidateContent(jtd2);
			}
			else
			{
				NUnit.Framework.Assert.IsFalse("Version 2 commits to output dir " + jtd2, jtd2.Exists
					());
				if (commitVersion == 1)
				{
					NUnit.Framework.Assert.IsTrue("Version 2  recovery moves to output dir from " + jtd
						, jtd.List().Length == 0);
				}
			}
			committer2.CommitJob(jContext2);
			ValidateContent(outDir);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoveryV1()
		{
			TestRecoveryInternal(1, 1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoveryV2()
		{
			TestRecoveryInternal(2, 2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoveryUpgradeV1V2()
		{
			TestRecoveryInternal(1, 2);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateContent(Path dir)
		{
			ValidateContent(new FilePath(dir.ToUri().GetPath()));
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateContent(FilePath dir)
		{
			FilePath expectedFile = new FilePath(dir, partFile);
			NUnit.Framework.Assert.IsTrue("Could not find " + expectedFile, expectedFile.Exists
				());
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append(key1).Append('\t').Append(val1).Append("\n");
			expectedOutput.Append(val1).Append("\n");
			expectedOutput.Append(val2).Append("\n");
			expectedOutput.Append(key2).Append("\n");
			expectedOutput.Append(key1).Append("\n");
			expectedOutput.Append(key2).Append('\t').Append(val2).Append("\n");
			string output = Slurp(expectedFile);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateMapFileOutputContent(FileSystem fs, Path dir)
		{
			// map output is a directory with index and data files
			Path expectedMapDir = new Path(dir, partFile);
			System.Diagnostics.Debug.Assert((fs.GetFileStatus(expectedMapDir).IsDirectory()));
			FileStatus[] files = fs.ListStatus(expectedMapDir);
			int fileCount = 0;
			bool dataFileFound = false;
			bool indexFileFound = false;
			foreach (FileStatus f in files)
			{
				if (f.IsFile())
				{
					++fileCount;
					if (f.GetPath().GetName().Equals(MapFile.IndexFileName))
					{
						indexFileFound = true;
					}
					else
					{
						if (f.GetPath().GetName().Equals(MapFile.DataFileName))
						{
							dataFileFound = true;
						}
					}
				}
			}
			System.Diagnostics.Debug.Assert((fileCount > 0));
			System.Diagnostics.Debug.Assert((dataFileFound && indexFileFound));
		}

		/// <exception cref="System.Exception"/>
		private void TestCommitterInternal(int version)
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			WriteOutput(theRecordWriter, tContext);
			// do commit
			committer.CommitTask(tContext);
			committer.CommitJob(jContext);
			// validate output
			ValidateContent(outDir);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitterV1()
		{
			TestCommitterInternal(1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitterV2()
		{
			TestCommitterInternal(2);
		}

		/// <exception cref="System.Exception"/>
		private void TestMapFileOutputCommitterInternal(int version)
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			MapFileOutputFormat theOutputFormat = new MapFileOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			WriteMapFileOutput(theRecordWriter, tContext);
			// do commit
			committer.CommitTask(tContext);
			committer.CommitJob(jContext);
			// validate output
			ValidateMapFileOutputContent(FileSystem.Get(job.GetConfiguration()), outDir);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapFileOutputCommitterV1()
		{
			TestMapFileOutputCommitterInternal(1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapFileOutputCommitterV2()
		{
			TestMapFileOutputCommitterInternal(2);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInvalidVersionNumber()
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, 3);
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			try
			{
				new FileOutputCommitter(outDir, tContext);
				Fail("should've thrown an exception!");
			}
			catch (IOException)
			{
			}
		}

		//test passed
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TestAbortInternal(int version)
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			WriteOutput(theRecordWriter, tContext);
			// do abort
			committer.AbortTask(tContext);
			FilePath expectedFile = new FilePath(new Path(committer.GetWorkPath(), partFile).
				ToString());
			NUnit.Framework.Assert.IsFalse("task temp dir still exists", expectedFile.Exists(
				));
			committer.AbortJob(jContext, JobStatus.State.Failed);
			expectedFile = new FilePath(new Path(outDir, FileOutputCommitter.PendingDirName).
				ToString());
			NUnit.Framework.Assert.IsFalse("job temp dir still exists", expectedFile.Exists()
				);
			NUnit.Framework.Assert.AreEqual("Output directory not empty", 0, new FilePath(outDir
				.ToString()).ListFiles().Length);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAbortV1()
		{
			TestAbortInternal(1);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAbortV2()
		{
			TestAbortInternal(2);
		}

		public class FakeFileSystem : RawLocalFileSystem
		{
			public FakeFileSystem()
				: base()
			{
			}

			public override URI GetUri()
			{
				return URI.Create("faildel:///");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path p, bool recursive)
			{
				throw new IOException("fake delete failed");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TestFailAbortInternal(int version)
		{
			Job job = Job.GetInstance();
			Configuration conf = job.GetConfiguration();
			conf.Set(FileSystem.FsDefaultNameKey, "faildel:///");
			conf.SetClass("fs.faildel.impl", typeof(TestFileOutputCommitter.FakeFileSystem), 
				typeof(FileSystem));
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(MRJobConfig.ApplicationAttemptId, 1);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			FileOutputFormat.SetOutputPath(job, outDir);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat();
			RecordWriter<object, object> theRecordWriter = theOutputFormat.GetRecordWriter(tContext
				);
			WriteOutput(theRecordWriter, tContext);
			// do abort
			Exception th = null;
			try
			{
				committer.AbortTask(tContext);
			}
			catch (IOException ie)
			{
				th = ie;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			NUnit.Framework.Assert.IsTrue(th is IOException);
			NUnit.Framework.Assert.IsTrue(th.Message.Contains("fake delete failed"));
			Path jtd = committer.GetJobAttemptPath(jContext);
			FilePath jobTmpDir = new FilePath(jtd.ToUri().GetPath());
			Path ttd = committer.GetTaskAttemptPath(tContext);
			FilePath taskTmpDir = new FilePath(ttd.ToUri().GetPath());
			FilePath expectedFile = new FilePath(taskTmpDir, partFile);
			NUnit.Framework.Assert.IsTrue(expectedFile + " does not exists", expectedFile.Exists
				());
			th = null;
			try
			{
				committer.AbortJob(jContext, JobStatus.State.Failed);
			}
			catch (IOException ie)
			{
				th = ie;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			NUnit.Framework.Assert.IsTrue(th is IOException);
			NUnit.Framework.Assert.IsTrue(th.Message.Contains("fake delete failed"));
			NUnit.Framework.Assert.IsTrue("job temp dir does not exists", jobTmpDir.Exists());
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailAbortV1()
		{
			TestFailAbortInternal(1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailAbortV2()
		{
			TestFailAbortInternal(2);
		}

		internal class RLFS : RawLocalFileSystem
		{
			private sealed class _ThreadLocal_464 : ThreadLocal<bool>
			{
				public _ThreadLocal_464()
				{
				}

				protected override bool InitialValue()
				{
					return true;
				}
			}

			private readonly ThreadLocal<bool> needNull = new _ThreadLocal_464();

			public RLFS()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				if (needNull.Get() && OutSubDir.ToUri().GetPath().Equals(f.ToUri().GetPath()))
				{
					needNull.Set(false);
					// lie once per thread
					return null;
				}
				return base.GetFileStatus(f);
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestConcurrentCommitTaskWithSubDir(int version)
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			conf.SetClass("fs.file.impl", typeof(TestFileOutputCommitter.RLFS), typeof(FileSystem
				));
			FileSystem.CloseAll();
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			FileOutputCommitter amCommitter = new FileOutputCommitter(outDir, jContext);
			amCommitter.SetupJob(jContext);
			TaskAttemptContext[] taCtx = new TaskAttemptContextImpl[2];
			taCtx[0] = new TaskAttemptContextImpl(conf, taskID);
			taCtx[1] = new TaskAttemptContextImpl(conf, taskID1);
			TextOutputFormat[] tof = new TextOutputFormat[2];
			for (int i = 0; i < tof.Length; i++)
			{
				tof[i] = new _TextOutputFormat_508(this);
			}
			ExecutorService executor = Executors.NewFixedThreadPool(2);
			try
			{
				for (int i_1 = 0; i_1 < taCtx.Length; i_1++)
				{
					int taskIdx = i_1;
					executor.Submit(new _Callable_524(this, tof, taskIdx, taCtx));
				}
			}
			finally
			{
				executor.Shutdown();
				while (!executor.AwaitTermination(1, TimeUnit.Seconds))
				{
					Log.Info("Awaiting thread termination!");
				}
			}
			amCommitter.CommitJob(jContext);
			RawLocalFileSystem lfs = new RawLocalFileSystem();
			lfs.SetConf(conf);
			NUnit.Framework.Assert.IsFalse("Must not end up with sub_dir/sub_dir", lfs.Exists
				(new Path(OutSubDir, SubDir)));
			// validate output
			ValidateContent(OutSubDir);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		private sealed class _TextOutputFormat_508 : TextOutputFormat
		{
			public _TextOutputFormat_508(TestFileOutputCommitter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetDefaultWorkFile(TaskAttemptContext context, string extension
				)
			{
				FileOutputCommitter foc = (FileOutputCommitter)this._enclosing._enclosing.GetOutputCommitter
					(context);
				return new Path(new Path(foc.GetWorkPath(), TestFileOutputCommitter.SubDir), FileOutputFormat
					.GetUniqueFile(context, FileOutputFormat.GetOutputName(context), extension));
			}

			private readonly TestFileOutputCommitter _enclosing;
		}

		private sealed class _Callable_524 : Callable<Void>
		{
			public _Callable_524(TestFileOutputCommitter _enclosing, TextOutputFormat[] tof, 
				int taskIdx, TaskAttemptContext[] taCtx)
			{
				this._enclosing = _enclosing;
				this.tof = tof;
				this.taskIdx = taskIdx;
				this.taCtx = taCtx;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				OutputCommitter outputCommitter = tof[taskIdx].GetOutputCommitter(taCtx[taskIdx]);
				outputCommitter.SetupTask(taCtx[taskIdx]);
				RecordWriter rw = tof[taskIdx].GetRecordWriter(taCtx[taskIdx]);
				this._enclosing.WriteOutput(rw, taCtx[taskIdx]);
				outputCommitter.CommitTask(taCtx[taskIdx]);
				return null;
			}

			private readonly TestFileOutputCommitter _enclosing;

			private readonly TextOutputFormat[] tof;

			private readonly int taskIdx;

			private readonly TaskAttemptContext[] taCtx;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConcurrentCommitTaskWithSubDirV1()
		{
			TestConcurrentCommitTaskWithSubDir(1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConcurrentCommitTaskWithSubDirV2()
		{
			TestConcurrentCommitTaskWithSubDir(2);
		}

		/// <exception cref="System.IO.IOException"/>
		public static string Slurp(FilePath f)
		{
			int len = (int)f.Length();
			byte[] buf = new byte[len];
			FileInputStream @in = new FileInputStream(f);
			string contents = null;
			try
			{
				@in.Read(buf, 0, len);
				contents = Sharpen.Runtime.GetStringForBytes(buf, "UTF-8");
			}
			finally
			{
				@in.Close();
			}
			return contents;
		}
	}
}
