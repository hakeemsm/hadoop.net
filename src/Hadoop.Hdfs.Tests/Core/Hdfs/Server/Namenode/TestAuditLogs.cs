using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A JUnit test that audit logs are generated</summary>
	public class TestAuditLogs
	{
		internal static readonly string auditLogFile = PathUtils.GetTestDirName(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestAuditLogs
			)) + "/TestAuditLogs-audit.log";

		internal readonly bool useAsyncLog;

		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			ICollection<object[]> @params = new AList<object[]>();
			@params.AddItem(new object[] { false });
			@params.AddItem(new object[] { true });
			return @params;
		}

		public TestAuditLogs(bool useAsyncLog)
		{
			this.useAsyncLog = useAsyncLog;
		}

		internal static readonly Sharpen.Pattern auditPattern = Sharpen.Pattern.Compile("allowed=.*?\\s"
			 + "ugi=.*?\\s" + "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + "cmd=.*?\\ssrc=.*?\\sdst=null\\s"
			 + "perm=.*?");

		internal static readonly Sharpen.Pattern successPattern = Sharpen.Pattern.Compile
			(".*allowed=true.*");

		internal static readonly Sharpen.Pattern webOpenPattern = Sharpen.Pattern.Compile
			(".*cmd=open.*proto=webhdfs.*");

		internal const string username = "bob";

		internal static readonly string[] groups = new string[] { "group1" };

		internal const string fileName = "/srcdat";

		internal DFSTestUtil util;

		internal MiniDFSCluster cluster;

		internal FileSystem fs;

		internal string[] fnames;

		internal Configuration conf;

		internal UserGroupInformation userGroupInfo;

		// Pattern for: 
		// allowed=(true|false) ugi=name ip=/address cmd={cmd} src={path} dst=null perm=null
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			// must configure prior to instantiating the namesystem because it
			// will reconfigure the logger if async is enabled
			ConfigureAuditLogs();
			conf = new HdfsConfiguration();
			long precision = 1L;
			conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, precision);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAuditLogAsyncKey, useAsyncLog);
			util = new DFSTestUtil.Builder().SetName("TestAuditAllowed").SetNumFiles(20).Build
				();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
			fs = cluster.GetFileSystem();
			util.CreateFiles(fs, fileName);
			// make sure the appender is what it's supposed to be
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			IList<Appender> appenders = Sharpen.Collections.List(logger.GetAllAppenders());
			NUnit.Framework.Assert.AreEqual(1, appenders.Count);
			NUnit.Framework.Assert.AreEqual(useAsyncLog, appenders[0] is AsyncAppender);
			fnames = util.GetFileNames(fileName);
			util.WaitReplication(fs, fileName, (short)3);
			userGroupInfo = UserGroupInformation.CreateUserForTesting(username, groups);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TeardownCluster()
		{
			util.Cleanup(fs, "/srcdat");
			fs.Close();
			cluster.Shutdown();
		}

		/// <summary>test that allowed operation puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditAllowed()
		{
			Path file = new Path(fnames[0]);
			FileSystem userfs = DFSTestUtil.GetFileSystemAs(userGroupInfo, conf);
			SetupAuditLogs();
			InputStream istream = userfs.Open(file);
			int val = istream.Read();
			istream.Close();
			VerifyAuditLogs(true);
			NUnit.Framework.Assert.IsTrue("failed to read from file", val >= 0);
		}

		/// <summary>test that allowed stat puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditAllowedStat()
		{
			Path file = new Path(fnames[0]);
			FileSystem userfs = DFSTestUtil.GetFileSystemAs(userGroupInfo, conf);
			SetupAuditLogs();
			FileStatus st = userfs.GetFileStatus(file);
			VerifyAuditLogs(true);
			NUnit.Framework.Assert.IsTrue("failed to stat file", st != null && st.IsFile());
		}

		/// <summary>test that denied operation puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditDenied()
		{
			Path file = new Path(fnames[0]);
			FileSystem userfs = DFSTestUtil.GetFileSystemAs(userGroupInfo, conf);
			fs.SetPermission(file, new FsPermission((short)0x180));
			fs.SetOwner(file, "root", null);
			SetupAuditLogs();
			try
			{
				userfs.Open(file);
				NUnit.Framework.Assert.Fail("open must not succeed");
			}
			catch (AccessControlException)
			{
				System.Console.Out.WriteLine("got access denied, as expected.");
			}
			VerifyAuditLogs(false);
		}

		/// <summary>test that access via webhdfs puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditWebHdfs()
		{
			Path file = new Path(fnames[0]);
			fs.SetPermission(file, new FsPermission((short)0x1a4));
			fs.SetOwner(file, "root", null);
			SetupAuditLogs();
			WebHdfsFileSystem webfs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(userGroupInfo, conf
				, WebHdfsFileSystem.Scheme);
			InputStream istream = webfs.Open(file);
			int val = istream.Read();
			istream.Close();
			VerifyAuditLogsRepeat(true, 3);
			NUnit.Framework.Assert.IsTrue("failed to read from file", val >= 0);
		}

		/// <summary>test that stat via webhdfs puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditWebHdfsStat()
		{
			Path file = new Path(fnames[0]);
			fs.SetPermission(file, new FsPermission((short)0x1a4));
			fs.SetOwner(file, "root", null);
			SetupAuditLogs();
			WebHdfsFileSystem webfs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(userGroupInfo, conf
				, WebHdfsFileSystem.Scheme);
			FileStatus st = webfs.GetFileStatus(file);
			VerifyAuditLogs(true);
			NUnit.Framework.Assert.IsTrue("failed to stat file", st != null && st.IsFile());
		}

		/// <summary>test that access via Hftp puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditHftp()
		{
			Path file = new Path(fnames[0]);
			string hftpUri = "hftp://" + conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey);
			HftpFileSystem hftpFs = null;
			SetupAuditLogs();
			try
			{
				hftpFs = (HftpFileSystem)new Path(hftpUri).GetFileSystem(conf);
				InputStream istream = hftpFs.Open(file);
				int val = istream.Read();
				istream.Close();
				VerifyAuditLogs(true);
			}
			finally
			{
				if (hftpFs != null)
				{
					hftpFs.Close();
				}
			}
		}

		/// <summary>test that denied access via webhdfs puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditWebHdfsDenied()
		{
			Path file = new Path(fnames[0]);
			fs.SetPermission(file, new FsPermission((short)0x180));
			fs.SetOwner(file, "root", null);
			SetupAuditLogs();
			try
			{
				WebHdfsFileSystem webfs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(userGroupInfo, conf
					, WebHdfsFileSystem.Scheme);
				InputStream istream = webfs.Open(file);
				int val = istream.Read();
				NUnit.Framework.Assert.Fail("open+read must not succeed, got " + val);
			}
			catch (AccessControlException)
			{
				System.Console.Out.WriteLine("got access denied, as expected.");
			}
			VerifyAuditLogsRepeat(false, 2);
		}

		/// <summary>test that open via webhdfs puts proper entry in audit log</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditWebHdfsOpen()
		{
			Path file = new Path(fnames[0]);
			fs.SetPermission(file, new FsPermission((short)0x1a4));
			fs.SetOwner(file, "root", null);
			SetupAuditLogs();
			WebHdfsFileSystem webfs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(userGroupInfo, conf
				, WebHdfsFileSystem.Scheme);
			webfs.Open(file);
			VerifyAuditLogsCheckPattern(true, 3, webOpenPattern);
		}

		/// <summary>Sets up log4j logger for auditlogs</summary>
		/// <exception cref="System.IO.IOException"/>
		private void SetupAuditLogs()
		{
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			// enable logging now that the test is ready to run
			logger.SetLevel(Level.Info);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ConfigureAuditLogs()
		{
			// Shutdown the LogManager to release all logger open file handles.
			// Unfortunately, Apache commons logging library does not provide
			// means to release underlying loggers. For additional info look up
			// commons library FAQ.
			LogManager.Shutdown();
			FilePath file = new FilePath(auditLogFile);
			if (file.Exists())
			{
				NUnit.Framework.Assert.IsTrue(file.Delete());
			}
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			// disable logging while the cluster startup preps files
			logger.SetLevel(Level.Off);
			PatternLayout layout = new PatternLayout("%m%n");
			RollingFileAppender appender = new RollingFileAppender(layout, auditLogFile);
			logger.AddAppender(appender);
		}

		// Ensure audit log has only one entry
		/// <exception cref="System.IO.IOException"/>
		private void VerifyAuditLogs(bool expectSuccess)
		{
			VerifyAuditLogsRepeat(expectSuccess, 1);
		}

		// Ensure audit log has exactly N entries
		/// <exception cref="System.IO.IOException"/>
		private void VerifyAuditLogsRepeat(bool expectSuccess, int ndupe)
		{
			// Turn off the logs
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			logger.SetLevel(Level.Off);
			// Close the appenders and force all logs to be flushed
			Enumeration<object> appenders = logger.GetAllAppenders();
			while (appenders.MoveNext())
			{
				Appender appender = (Appender)appenders.Current;
				appender.Close();
			}
			BufferedReader reader = new BufferedReader(new FileReader(auditLogFile));
			string line = null;
			bool ret = true;
			try
			{
				for (int i = 0; i < ndupe; i++)
				{
					line = reader.ReadLine();
					NUnit.Framework.Assert.IsNotNull(line);
					NUnit.Framework.Assert.IsTrue("Expected audit event not found in audit log", auditPattern
						.Matcher(line).Matches());
					ret &= successPattern.Matcher(line).Matches();
				}
				NUnit.Framework.Assert.IsNull("Unexpected event in audit log", reader.ReadLine());
				NUnit.Framework.Assert.IsTrue("Expected success=" + expectSuccess, ret == expectSuccess
					);
			}
			finally
			{
				reader.Close();
			}
		}

		// Ensure audit log has exactly N entries
		/// <exception cref="System.IO.IOException"/>
		private void VerifyAuditLogsCheckPattern(bool expectSuccess, int ndupe, Sharpen.Pattern
			 pattern)
		{
			// Turn off the logs
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			logger.SetLevel(Level.Off);
			// Close the appenders and force all logs to be flushed
			Enumeration<object> appenders = logger.GetAllAppenders();
			while (appenders.MoveNext())
			{
				Appender appender = (Appender)appenders.Current;
				appender.Close();
			}
			BufferedReader reader = new BufferedReader(new FileReader(auditLogFile));
			string line = null;
			bool ret = true;
			bool patternMatches = false;
			try
			{
				for (int i = 0; i < ndupe; i++)
				{
					line = reader.ReadLine();
					NUnit.Framework.Assert.IsNotNull(line);
					patternMatches |= pattern.Matcher(line).Matches();
					ret &= successPattern.Matcher(line).Matches();
				}
				NUnit.Framework.Assert.IsNull("Unexpected event in audit log", reader.ReadLine());
				NUnit.Framework.Assert.IsTrue("Expected audit event not found in audit log", patternMatches
					);
				NUnit.Framework.Assert.IsTrue("Expected success=" + expectSuccess, ret == expectSuccess
					);
			}
			finally
			{
				reader.Close();
			}
		}
	}
}
