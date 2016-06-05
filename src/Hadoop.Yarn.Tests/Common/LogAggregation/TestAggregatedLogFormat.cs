using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	public class TestAggregatedLogFormat
	{
		private static readonly FilePath testWorkDir = new FilePath("target", "TestAggregatedLogFormat"
			);

		private static readonly Configuration conf = new Configuration();

		private static readonly FileSystem fs;

		private const char filler = 'x';

		private static readonly Log Log = LogFactory.GetLog(typeof(TestAggregatedLogFormat
			));

		static TestAggregatedLogFormat()
		{
			try
			{
				fs = FileSystem.Get(conf);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		[TearDown]
		public virtual void CleanupTestDir()
		{
			Path workDirPath = new Path(testWorkDir.GetAbsolutePath());
			Log.Info("Cleaning test directory [" + workDirPath + "]");
			fs.Delete(workDirPath, true);
		}

		//Test for Corrupted AggregatedLogs. The Logs should not write more data
		//if Logvalue.write() is called and the application is still
		//appending to logs
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestForCorruptedAggregatedLogs()
		{
			Configuration conf = new Configuration();
			FilePath workDir = new FilePath(testWorkDir, "testReadAcontainerLogs1");
			Path remoteAppLogFile = new Path(workDir.GetAbsolutePath(), "aggregatedLogFile");
			Path srcFileRoot = new Path(workDir.GetAbsolutePath(), "srcFiles");
			ContainerId testContainerId = TestContainerId.NewContainerId(1, 1, 1, 1);
			Path t = new Path(srcFileRoot, testContainerId.GetApplicationAttemptId().GetApplicationId
				().ToString());
			Path srcFilePath = new Path(t, testContainerId.ToString());
			long numChars = 950000;
			WriteSrcFileAndALog(srcFilePath, "stdout", numChars, remoteAppLogFile, srcFileRoot
				, testContainerId);
			AggregatedLogFormat.LogReader logReader = new AggregatedLogFormat.LogReader(conf, 
				remoteAppLogFile);
			AggregatedLogFormat.LogKey rLogKey = new AggregatedLogFormat.LogKey();
			DataInputStream dis = logReader.Next(rLogKey);
			TextWriter writer = new StringWriter();
			try
			{
				AggregatedLogFormat.LogReader.ReadAcontainerLogs(dis, writer);
			}
			catch (Exception e)
			{
				if (e.ToString().Contains("NumberFormatException"))
				{
					NUnit.Framework.Assert.Fail("Aggregated logs are corrupted.");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void WriteSrcFileAndALog(Path srcFilePath, string fileName, long length, 
			Path remoteAppLogFile, Path srcFileRoot, ContainerId testContainerId)
		{
			FilePath dir = new FilePath(srcFilePath.ToString());
			if (!dir.Exists())
			{
				if (!dir.Mkdirs())
				{
					throw new IOException("Unable to create directory : " + dir);
				}
			}
			FilePath outputFile = new FilePath(new FilePath(srcFilePath.ToString()), fileName
				);
			FileOutputStream os = new FileOutputStream(outputFile);
			OutputStreamWriter osw = new OutputStreamWriter(os, "UTF8");
			int ch = filler;
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			AggregatedLogFormat.LogWriter logWriter = new AggregatedLogFormat.LogWriter(conf, 
				remoteAppLogFile, ugi);
			AggregatedLogFormat.LogKey logKey = new AggregatedLogFormat.LogKey(testContainerId
				);
			AggregatedLogFormat.LogValue logValue = Org.Mockito.Mockito.Spy(new AggregatedLogFormat.LogValue
				(Collections.SingletonList(srcFileRoot.ToString()), testContainerId, ugi.GetShortUserName
				()));
			CountDownLatch latch = new CountDownLatch(1);
			Sharpen.Thread t = new _Thread_152(length, osw, ch, latch);
			// TODO Auto-generated catch block
			t.Start();
			//Wait till the osw is partially written
			//aggregation starts once the ows has completed 1/3rd of its work
			latch.Await();
			//Aggregate The Logs
			logWriter.Append(logKey, logValue);
			logWriter.Close();
		}

		private sealed class _Thread_152 : Sharpen.Thread
		{
			public _Thread_152(long length, OutputStreamWriter osw, int ch, CountDownLatch latch
				)
			{
				this.length = length;
				this.osw = osw;
				this.ch = ch;
				this.latch = latch;
			}

			public override void Run()
			{
				try
				{
					for (int i = 0; i < length / 3; i++)
					{
						osw.Write(ch);
					}
					latch.CountDown();
					for (int i_1 = 0; i_1 < (2 * length) / 3; i_1++)
					{
						osw.Write(ch);
					}
					osw.Close();
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly long length;

			private readonly OutputStreamWriter osw;

			private readonly int ch;

			private readonly CountDownLatch latch;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadAcontainerLogs1()
		{
			//Verify the output generated by readAContainerLogs(DataInputStream, Writer, logUploadedTime)
			TestReadAcontainerLog(true);
			//Verify the output generated by readAContainerLogs(DataInputStream, Writer)
			TestReadAcontainerLog(false);
		}

		/// <exception cref="System.Exception"/>
		private void TestReadAcontainerLog(bool logUploadedTime)
		{
			Configuration conf = new Configuration();
			FilePath workDir = new FilePath(testWorkDir, "testReadAcontainerLogs1");
			Path remoteAppLogFile = new Path(workDir.GetAbsolutePath(), "aggregatedLogFile");
			Path srcFileRoot = new Path(workDir.GetAbsolutePath(), "srcFiles");
			ContainerId testContainerId = TestContainerId.NewContainerId(1, 1, 1, 1);
			Path t = new Path(srcFileRoot, testContainerId.GetApplicationAttemptId().GetApplicationId
				().ToString());
			Path srcFilePath = new Path(t, testContainerId.ToString());
			int numChars = 80000;
			// create a sub-folder under srcFilePath
			// and create file logs in this sub-folder.
			// We only aggregate top level files.
			// So, this log file should be ignored.
			Path subDir = new Path(srcFilePath, "subDir");
			fs.Mkdirs(subDir);
			WriteSrcFile(subDir, "logs", numChars);
			// create file stderr and stdout in containerLogDir
			WriteSrcFile(srcFilePath, "stderr", numChars);
			WriteSrcFile(srcFilePath, "stdout", numChars);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			AggregatedLogFormat.LogWriter logWriter = new AggregatedLogFormat.LogWriter(conf, 
				remoteAppLogFile, ugi);
			AggregatedLogFormat.LogKey logKey = new AggregatedLogFormat.LogKey(testContainerId
				);
			AggregatedLogFormat.LogValue logValue = new AggregatedLogFormat.LogValue(Collections
				.SingletonList(srcFileRoot.ToString()), testContainerId, ugi.GetShortUserName());
			// When we try to open FileInputStream for stderr, it will throw out an IOException.
			// Skip the log aggregation for stderr.
			AggregatedLogFormat.LogValue spyLogValue = Org.Mockito.Mockito.Spy(logValue);
			FilePath errorFile = new FilePath((new Path(srcFilePath, "stderr")).ToString());
			Org.Mockito.Mockito.DoThrow(new IOException("Mock can not open FileInputStream"))
				.When(spyLogValue).SecureOpenFile(errorFile);
			logWriter.Append(logKey, spyLogValue);
			logWriter.Close();
			// make sure permission are correct on the file
			FileStatus fsStatus = fs.GetFileStatus(remoteAppLogFile);
			NUnit.Framework.Assert.AreEqual("permissions on log aggregation file are wrong", 
				FsPermission.CreateImmutable((short)0x1a0), fsStatus.GetPermission());
			AggregatedLogFormat.LogReader logReader = new AggregatedLogFormat.LogReader(conf, 
				remoteAppLogFile);
			AggregatedLogFormat.LogKey rLogKey = new AggregatedLogFormat.LogKey();
			DataInputStream dis = logReader.Next(rLogKey);
			TextWriter writer = new StringWriter();
			if (logUploadedTime)
			{
				AggregatedLogFormat.LogReader.ReadAcontainerLogs(dis, writer, Runtime.CurrentTimeMillis
					());
			}
			else
			{
				AggregatedLogFormat.LogReader.ReadAcontainerLogs(dis, writer);
			}
			// We should only do the log aggregation for stdout.
			// Since we could not open the fileInputStream for stderr, this file is not
			// aggregated.
			string s = writer.ToString();
			int expectedLength = "LogType:stdout".Length + (logUploadedTime ? ("\nLog Upload Time:"
				 + Times.Format(Runtime.CurrentTimeMillis())).Length : 0) + ("\nLogLength:" + numChars
				).Length + "\nLog Contents:\n".Length + numChars + "\n".Length + "End of LogType:stdout\n"
				.Length;
			NUnit.Framework.Assert.IsTrue("LogType not matched", s.Contains("LogType:stdout")
				);
			NUnit.Framework.Assert.IsTrue("log file:stderr should not be aggregated.", !s.Contains
				("LogType:stderr"));
			NUnit.Framework.Assert.IsTrue("log file:logs should not be aggregated.", !s.Contains
				("LogType:logs"));
			NUnit.Framework.Assert.IsTrue("LogLength not matched", s.Contains("LogLength:" + 
				numChars));
			NUnit.Framework.Assert.IsTrue("Log Contents not matched", s.Contains("Log Contents"
				));
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < numChars; i++)
			{
				sb.Append(filler);
			}
			string expectedContent = sb.ToString();
			NUnit.Framework.Assert.IsTrue("Log content incorrect", s.Contains(expectedContent
				));
			NUnit.Framework.Assert.AreEqual(expectedLength, s.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerLogsFileAccess()
		{
			// This test will run only if NativeIO is enabled as SecureIOUtils 
			// require it to be enabled.
			Assume.AssumeTrue(NativeIO.IsAvailable());
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath workDir = new FilePath(testWorkDir, "testContainerLogsFileAccess1");
			Path remoteAppLogFile = new Path(workDir.GetAbsolutePath(), "aggregatedLogFile");
			Path srcFileRoot = new Path(workDir.GetAbsolutePath(), "srcFiles");
			string data = "Log File content for container : ";
			// Creating files for container1. Log aggregator will try to read log files
			// with illegal user.
			ApplicationId applicationId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ContainerId testContainerId1 = ContainerId.NewContainerId(applicationAttemptId, 1
				);
			Path appDir = new Path(srcFileRoot, testContainerId1.GetApplicationAttemptId().GetApplicationId
				().ToString());
			Path srcFilePath1 = new Path(appDir, testContainerId1.ToString());
			string stdout = "stdout";
			string stderr = "stderr";
			WriteSrcFile(srcFilePath1, stdout, data + testContainerId1.ToString() + stdout);
			WriteSrcFile(srcFilePath1, stderr, data + testContainerId1.ToString() + stderr);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			AggregatedLogFormat.LogWriter logWriter = new AggregatedLogFormat.LogWriter(conf, 
				remoteAppLogFile, ugi);
			AggregatedLogFormat.LogKey logKey = new AggregatedLogFormat.LogKey(testContainerId1
				);
			string randomUser = "randomUser";
			AggregatedLogFormat.LogValue logValue = Org.Mockito.Mockito.Spy(new AggregatedLogFormat.LogValue
				(Collections.SingletonList(srcFileRoot.ToString()), testContainerId1, randomUser
				));
			// It is trying simulate a situation where first log file is owned by
			// different user (probably symlink) and second one by the user itself.
			// The first file should not be aggregated. Because this log file has the invalid
			// user name.
			Org.Mockito.Mockito.When(logValue.GetUser()).ThenReturn(randomUser).ThenReturn(ugi
				.GetShortUserName());
			logWriter.Append(logKey, logValue);
			logWriter.Close();
			BufferedReader @in = new BufferedReader(new FileReader(new FilePath(remoteAppLogFile
				.ToUri().GetRawPath())));
			string line;
			StringBuilder sb = new StringBuilder(string.Empty);
			while ((line = @in.ReadLine()) != null)
			{
				Log.Info(line);
				sb.Append(line);
			}
			line = sb.ToString();
			string expectedOwner = ugi.GetShortUserName();
			if (Path.Windows)
			{
				string adminsGroupString = "Administrators";
				if (Arrays.AsList(ugi.GetGroupNames()).Contains(adminsGroupString))
				{
					expectedOwner = adminsGroupString;
				}
			}
			// This file: stderr should not be aggregated.
			// And we will not aggregate the log message.
			string stdoutFile1 = StringUtils.Join(FilePath.separator, Arrays.AsList(new string
				[] { workDir.GetAbsolutePath(), "srcFiles", testContainerId1.GetApplicationAttemptId
				().GetApplicationId().ToString(), testContainerId1.ToString(), stderr }));
			// The file: stdout is expected to be aggregated.
			string stdoutFile2 = StringUtils.Join(FilePath.separator, Arrays.AsList(new string
				[] { workDir.GetAbsolutePath(), "srcFiles", testContainerId1.GetApplicationAttemptId
				().GetApplicationId().ToString(), testContainerId1.ToString(), stdout }));
			string message2 = "Owner '" + expectedOwner + "' for path " + stdoutFile2 + " did not match expected owner '"
				 + ugi.GetShortUserName() + "'";
			NUnit.Framework.Assert.IsFalse(line.Contains(message2));
			NUnit.Framework.Assert.IsFalse(line.Contains(data + testContainerId1.ToString() +
				 stderr));
			NUnit.Framework.Assert.IsTrue(line.Contains(data + testContainerId1.ToString() + 
				stdout));
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteSrcFile(Path srcFilePath, string fileName, long length)
		{
			OutputStreamWriter osw = GetOutputStreamWriter(srcFilePath, fileName);
			int ch = filler;
			for (int i = 0; i < length; i++)
			{
				osw.Write(ch);
			}
			osw.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteSrcFile(Path srcFilePath, string fileName, string data)
		{
			OutputStreamWriter osw = GetOutputStreamWriter(srcFilePath, fileName);
			osw.Write(data);
			osw.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.UnsupportedEncodingException"/>
		private OutputStreamWriter GetOutputStreamWriter(Path srcFilePath, string fileName
			)
		{
			FilePath dir = new FilePath(srcFilePath.ToString());
			if (!dir.Exists())
			{
				if (!dir.Mkdirs())
				{
					throw new IOException("Unable to create directory : " + dir);
				}
			}
			FilePath outputFile = new FilePath(new FilePath(srcFilePath.ToString()), fileName
				);
			FileOutputStream os = new FileOutputStream(outputFile);
			OutputStreamWriter osw = new OutputStreamWriter(os, "UTF8");
			return osw;
		}
	}
}
