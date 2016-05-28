using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFileOutputCommitter : TestCase
	{
		private static Path outDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"
			), "output");

		private static string attempt = "attempt_200707121733_0001_m_000000_0";

		private static string partFile = "part-00000";

		private static TaskAttemptID taskID = ((TaskAttemptID)TaskAttemptID.ForName(attempt
			));

		private Text key1 = new Text("key1");

		private Text key2 = new Text("key2");

		private Text val1 = new Text("val1");

		private Text val2 = new Text("val2");

		// A random task attempt id for testing.
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
				theRecordWriter.Close(null);
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
				theRecordWriter.Close(null);
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestRecoveryInternal(int commitVersion, int recoveryVersion)
		{
			JobConf conf = new JobConf();
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.Set(JobContext.TaskAttemptId, attempt);
			conf.SetInt(MRConstants.ApplicationAttemptId, 1);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, commitVersion
				);
			JobContext jContext = new JobContextImpl(conf, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(null, conf, partFile
				, null);
			WriteOutput(theRecordWriter, tContext);
			// do commit
			if (committer.NeedsTaskCommit(tContext))
			{
				committer.CommitTask(tContext);
			}
			Path jobTempDir1 = committer.GetCommittedTaskPath(tContext);
			FilePath jtd1 = new FilePath(jobTempDir1.ToUri().GetPath());
			if (commitVersion == 1)
			{
				NUnit.Framework.Assert.IsTrue("Version 1 commits to temporary dir " + jtd1, jtd1.
					Exists());
				ValidateContent(jobTempDir1);
			}
			else
			{
				NUnit.Framework.Assert.IsFalse("Version 2 commits to output dir " + jtd1, jtd1.Exists
					());
			}
			//now while running the second app attempt,
			//recover the task output from first attempt
			JobConf conf2 = new JobConf(conf);
			conf2.Set(JobContext.TaskAttemptId, attempt);
			conf2.SetInt(MRConstants.ApplicationAttemptId, 2);
			conf2.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, recoveryVersion
				);
			JobContext jContext2 = new JobContextImpl(conf2, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2, taskID);
			FileOutputCommitter committer2 = new FileOutputCommitter();
			committer2.SetupJob(jContext2);
			committer2.RecoverTask(tContext2);
			Path jobTempDir2 = committer2.GetCommittedTaskPath(tContext2);
			FilePath jtd2 = new FilePath(jobTempDir2.ToUri().GetPath());
			if (recoveryVersion == 1)
			{
				NUnit.Framework.Assert.IsTrue("Version 1 recovers to " + jtd2, jtd2.Exists());
				ValidateContent(jobTempDir2);
			}
			else
			{
				NUnit.Framework.Assert.IsFalse("Version 2 commits to output dir " + jtd2, jtd2.Exists
					());
				if (commitVersion == 1)
				{
					NUnit.Framework.Assert.IsTrue("Version 2  recovery moves to output dir from " + jtd1
						, jtd1.List().Length == 0);
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
			FilePath fdir = new FilePath(dir.ToUri().GetPath());
			FilePath expectedFile = new FilePath(fdir, partFile);
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
			JobConf conf = new JobConf();
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.Set(JobContext.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(null, conf, partFile
				, null);
			WriteOutput(theRecordWriter, tContext);
			// do commit
			if (committer.NeedsTaskCommit(tContext))
			{
				committer.CommitTask(tContext);
			}
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
			JobConf conf = new JobConf();
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.Set(JobContext.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			MapFileOutputFormat theOutputFormat = new MapFileOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(null, conf, partFile
				, null);
			WriteMapFileOutput(theRecordWriter, tContext);
			// do commit
			if (committer.NeedsTaskCommit(tContext))
			{
				committer.CommitTask(tContext);
			}
			committer.CommitJob(jContext);
			// validate output
			ValidateMapFileOutputContent(FileSystem.Get(conf), outDir);
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

		/// <exception cref="System.Exception"/>
		public virtual void TestMapOnlyNoOutputV1()
		{
			TestMapOnlyNoOutputInternal(1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapOnlyNoOutputV2()
		{
			TestMapOnlyNoOutputInternal(2);
		}

		/// <exception cref="System.Exception"/>
		private void TestMapOnlyNoOutputInternal(int version)
		{
			JobConf conf = new JobConf();
			//This is not set on purpose. FileOutputFormat.setOutputPath(conf, outDir);
			conf.Set(JobContext.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			if (committer.NeedsTaskCommit(tContext))
			{
				// do commit
				committer.CommitTask(tContext);
			}
			committer.CommitJob(jContext);
			// validate output
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TestAbortInternal(int version)
		{
			JobConf conf = new JobConf();
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.Set(JobContext.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			JobContext jContext = new JobContextImpl(conf, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(null, conf, partFile
				, null);
			WriteOutput(theRecordWriter, tContext);
			// do abort
			committer.AbortTask(tContext);
			FilePath @out = new FilePath(outDir.ToUri().GetPath());
			Path workPath = committer.GetWorkPath(tContext, outDir);
			FilePath wp = new FilePath(workPath.ToUri().GetPath());
			FilePath expectedFile = new FilePath(wp, partFile);
			NUnit.Framework.Assert.IsFalse("task temp dir still exists", expectedFile.Exists(
				));
			committer.AbortJob(jContext, JobStatus.State.Failed);
			expectedFile = new FilePath(@out, FileOutputCommitter.TempDirName);
			NUnit.Framework.Assert.IsFalse("job temp dir still exists", expectedFile.Exists()
				);
			NUnit.Framework.Assert.AreEqual("Output directory not empty", 0, @out.ListFiles()
				.Length);
			FileUtil.FullyDelete(@out);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAbortV1()
		{
			TestAbortInternal(1);
		}

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
			JobConf conf = new JobConf();
			conf.Set(FileSystem.FsDefaultNameKey, "faildel:///");
			conf.SetClass("fs.faildel.impl", typeof(TestFileOutputCommitter.FakeFileSystem), 
				typeof(FileSystem));
			conf.Set(JobContext.TaskAttemptId, attempt);
			conf.SetInt(FileOutputCommitter.FileoutputcommitterAlgorithmVersion, version);
			conf.SetInt(MRConstants.ApplicationAttemptId, 1);
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobContext jContext = new JobContextImpl(conf, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			FilePath jobTmpDir = new FilePath(new Path(outDir, FileOutputCommitter.TempDirName
				 + Path.Separator + conf.GetInt(MRConstants.ApplicationAttemptId, 0) + Path.Separator
				 + FileOutputCommitter.TempDirName).ToString());
			FilePath taskTmpDir = new FilePath(jobTmpDir, "_" + taskID);
			FilePath expectedFile = new FilePath(taskTmpDir, partFile);
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat();
			RecordWriter<object, object> theRecordWriter = theOutputFormat.GetRecordWriter(null
				, conf, expectedFile.GetAbsolutePath(), null);
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
