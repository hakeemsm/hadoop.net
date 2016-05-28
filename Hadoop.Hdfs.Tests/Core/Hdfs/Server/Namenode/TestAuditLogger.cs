using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Tests for the
	/// <see cref="AuditLogger"/>
	/// custom audit logging interface.
	/// </summary>
	public class TestAuditLogger
	{
		private const short TestPermission = (short)0x1ac;

		[SetUp]
		public virtual void Setup()
		{
			TestAuditLogger.DummyAuditLogger.initialized = false;
			TestAuditLogger.DummyAuditLogger.logCount = 0;
			TestAuditLogger.DummyAuditLogger.remoteAddr = null;
			Configuration conf = new HdfsConfiguration();
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
		}

		/// <summary>Tests that AuditLogger works as expected.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditLogger()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeAuditLoggersKey, typeof(TestAuditLogger.DummyAuditLogger
				).FullName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitClusterUp();
				NUnit.Framework.Assert.IsTrue(TestAuditLogger.DummyAuditLogger.initialized);
				TestAuditLogger.DummyAuditLogger.ResetLogCount();
				FileSystem fs = cluster.GetFileSystem();
				long time = Runtime.CurrentTimeMillis();
				fs.SetTimes(new Path("/"), time, time);
				NUnit.Framework.Assert.AreEqual(1, TestAuditLogger.DummyAuditLogger.logCount);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Tests that TopAuditLogger can be disabled</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDisableTopAuditLogger()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.NntopEnabledKey, false);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitClusterUp();
				IList<AuditLogger> auditLoggers = cluster.GetNameNode().GetNamesystem().GetAuditLoggers
					();
				foreach (AuditLogger auditLogger in auditLoggers)
				{
					NUnit.Framework.Assert.IsFalse("top audit logger is still hooked in after it is disabled"
						, auditLogger is TopAuditLogger);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsAuditLogger()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeAuditLoggersKey, typeof(TestAuditLogger.DummyAuditLogger
				).FullName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			GetOpParam.OP op = GetOpParam.OP.Getfilestatus;
			try
			{
				cluster.WaitClusterUp();
				NUnit.Framework.Assert.IsTrue(TestAuditLogger.DummyAuditLogger.initialized);
				URI uri = new URI("http", NetUtils.GetHostPortString(cluster.GetNameNode().GetHttpAddress
					()), "/webhdfs/v1/", op.ToQueryString(), null);
				// non-proxy request
				HttpURLConnection conn = (HttpURLConnection)uri.ToURL().OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
				conn.Disconnect();
				NUnit.Framework.Assert.AreEqual(1, TestAuditLogger.DummyAuditLogger.logCount);
				NUnit.Framework.Assert.AreEqual("127.0.0.1", TestAuditLogger.DummyAuditLogger.remoteAddr
					);
				// non-trusted proxied request
				conn = (HttpURLConnection)uri.ToURL().OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetRequestProperty("X-Forwarded-For", "1.1.1.1");
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
				conn.Disconnect();
				NUnit.Framework.Assert.AreEqual(2, TestAuditLogger.DummyAuditLogger.logCount);
				NUnit.Framework.Assert.AreEqual("127.0.0.1", TestAuditLogger.DummyAuditLogger.remoteAddr
					);
				// trusted proxied request
				conf.Set(ProxyServers.ConfHadoopProxyservers, "127.0.0.1");
				ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
				conn = (HttpURLConnection)uri.ToURL().OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetRequestProperty("X-Forwarded-For", "1.1.1.1");
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
				conn.Disconnect();
				NUnit.Framework.Assert.AreEqual(3, TestAuditLogger.DummyAuditLogger.logCount);
				NUnit.Framework.Assert.AreEqual("1.1.1.1", TestAuditLogger.DummyAuditLogger.remoteAddr
					);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Minor test related to HADOOP-9155.</summary>
		/// <remarks>
		/// Minor test related to HADOOP-9155. Verify that during a
		/// FileSystem.setPermission() operation, the stat passed in during the
		/// logAuditEvent() call returns the new permission rather than the old
		/// permission.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditLoggerWithSetPermission()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeAuditLoggersKey, typeof(TestAuditLogger.DummyAuditLogger
				).FullName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitClusterUp();
				NUnit.Framework.Assert.IsTrue(TestAuditLogger.DummyAuditLogger.initialized);
				TestAuditLogger.DummyAuditLogger.ResetLogCount();
				FileSystem fs = cluster.GetFileSystem();
				long time = Runtime.CurrentTimeMillis();
				Path p = new Path("/");
				fs.SetTimes(p, time, time);
				fs.SetPermission(p, new FsPermission(TestPermission));
				NUnit.Framework.Assert.AreEqual(TestPermission, TestAuditLogger.DummyAuditLogger.
					foundPermission);
				NUnit.Framework.Assert.AreEqual(2, TestAuditLogger.DummyAuditLogger.logCount);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuditLogWithAclFailure()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.Set(DFSConfigKeys.DfsNamenodeAuditLoggersKey, typeof(TestAuditLogger.DummyAuditLogger
				).FullName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitClusterUp();
				FSDirectory dir = cluster.GetNamesystem().GetFSDirectory();
				FSDirectory mockedDir = Org.Mockito.Mockito.Spy(dir);
				AccessControlException ex = new AccessControlException();
				Org.Mockito.Mockito.DoThrow(ex).When(mockedDir).GetPermissionChecker();
				cluster.GetNamesystem().SetFSDirectory(mockedDir);
				NUnit.Framework.Assert.IsTrue(TestAuditLogger.DummyAuditLogger.initialized);
				TestAuditLogger.DummyAuditLogger.ResetLogCount();
				FileSystem fs = cluster.GetFileSystem();
				Path p = new Path("/");
				IList<AclEntry> acls = Lists.NewArrayList();
				try
				{
					fs.GetAclStatus(p);
				}
				catch (AccessControlException)
				{
				}
				try
				{
					fs.SetAcl(p, acls);
				}
				catch (AccessControlException)
				{
				}
				try
				{
					fs.RemoveAcl(p);
				}
				catch (AccessControlException)
				{
				}
				try
				{
					fs.RemoveDefaultAcl(p);
				}
				catch (AccessControlException)
				{
				}
				try
				{
					fs.RemoveAclEntries(p, acls);
				}
				catch (AccessControlException)
				{
				}
				try
				{
					fs.ModifyAclEntries(p, acls);
				}
				catch (AccessControlException)
				{
				}
				NUnit.Framework.Assert.AreEqual(6, TestAuditLogger.DummyAuditLogger.logCount);
				NUnit.Framework.Assert.AreEqual(6, TestAuditLogger.DummyAuditLogger.unsuccessfulCount
					);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Tests that a broken audit logger causes requests to fail.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBrokenLogger()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeAuditLoggersKey, typeof(TestAuditLogger.BrokenAuditLogger
				).FullName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitClusterUp();
				FileSystem fs = cluster.GetFileSystem();
				long time = Runtime.CurrentTimeMillis();
				fs.SetTimes(new Path("/"), time, time);
				NUnit.Framework.Assert.Fail("Expected exception due to broken audit logger.");
			}
			catch (RemoteException)
			{
			}
			finally
			{
				// Expected.
				cluster.Shutdown();
			}
		}

		public class DummyAuditLogger : AuditLogger
		{
			internal static bool initialized;

			internal static int logCount;

			internal static int unsuccessfulCount;

			internal static short foundPermission;

			internal static string remoteAddr;

			public virtual void Initialize(Configuration conf)
			{
				initialized = true;
			}

			public static void ResetLogCount()
			{
				logCount = 0;
				unsuccessfulCount = 0;
			}

			public virtual void LogAuditEvent(bool succeeded, string userName, IPAddress addr
				, string cmd, string src, string dst, FileStatus stat)
			{
				remoteAddr = addr.GetHostAddress();
				logCount++;
				if (!succeeded)
				{
					unsuccessfulCount++;
				}
				if (stat != null)
				{
					foundPermission = stat.GetPermission().ToShort();
				}
			}
		}

		public class BrokenAuditLogger : AuditLogger
		{
			public virtual void Initialize(Configuration conf)
			{
			}

			// No op.
			public virtual void LogAuditEvent(bool succeeded, string userName, IPAddress addr
				, string cmd, string src, string dst, FileStatus stat)
			{
				throw new RuntimeException("uh oh");
			}
		}
	}
}
