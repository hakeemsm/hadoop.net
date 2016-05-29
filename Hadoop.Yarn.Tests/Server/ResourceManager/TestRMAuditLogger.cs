using System.Net;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>
	/// Tests
	/// <see cref="RMAuditLogger"/>
	/// .
	/// </summary>
	public class TestRMAuditLogger
	{
		private const string User = "test";

		private const string Operation = "oper";

		private const string Target = "tgt";

		private const string Perm = "admin group";

		private const string Desc = "description of an audit log";

		private static readonly ApplicationId Appid = Org.Mockito.Mockito.Mock<ApplicationId
			>();

		private static readonly ApplicationAttemptId Attemptid = Org.Mockito.Mockito.Mock
			<ApplicationAttemptId>();

		private static readonly ContainerId Containerid = Org.Mockito.Mockito.Mock<ContainerId
			>();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Org.Mockito.Mockito.When(Appid.ToString()).ThenReturn("app_1");
			Org.Mockito.Mockito.When(Attemptid.ToString()).ThenReturn("app_attempt_1");
			Org.Mockito.Mockito.When(Containerid.ToString()).ThenReturn("container_1");
		}

		/// <summary>Test the AuditLog format with key-val pair.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKeyValLogFormat()
		{
			StringBuilder actLog = new StringBuilder();
			StringBuilder expLog = new StringBuilder();
			// add the first k=v pair and check
			RMAuditLogger.Start(RMAuditLogger.Keys.User, User, actLog);
			expLog.Append("USER=test");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
			// append another k1=v1 pair to already added k=v and test
			RMAuditLogger.Add(RMAuditLogger.Keys.Operation, Operation, actLog);
			expLog.Append("\tOPERATION=oper");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
			// append another k1=null pair and test
			RMAuditLogger.Add(RMAuditLogger.Keys.Appid, (string)null, actLog);
			expLog.Append("\tAPPID=null");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
			// now add the target and check of the final string
			RMAuditLogger.Add(RMAuditLogger.Keys.Target, Target, actLog);
			expLog.Append("\tTARGET=tgt");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
		}

		/// <summary>Test the AuditLog format for successful events.</summary>
		private void TestSuccessLogFormatHelper(bool checkIP, ApplicationId appId, ApplicationAttemptId
			 attemptId, ContainerId containerId)
		{
			string sLog = RMAuditLogger.CreateSuccessLog(User, Operation, Target, appId, attemptId
				, containerId);
			StringBuilder expLog = new StringBuilder();
			expLog.Append("USER=test\t");
			if (checkIP)
			{
				IPAddress ip = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
				expLog.Append(RMAuditLogger.Keys.Ip.ToString() + "=" + ip.GetHostAddress() + "\t"
					);
			}
			expLog.Append("OPERATION=oper\tTARGET=tgt\tRESULT=SUCCESS");
			if (appId != null)
			{
				expLog.Append("\tAPPID=app_1");
			}
			if (attemptId != null)
			{
				expLog.Append("\tAPPATTEMPTID=app_attempt_1");
			}
			if (containerId != null)
			{
				expLog.Append("\tCONTAINERID=container_1");
			}
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), sLog);
		}

		/// <summary>Test the AuditLog format for successful events passing nulls.</summary>
		private void TestSuccessLogNulls(bool checkIP)
		{
			string sLog = RMAuditLogger.CreateSuccessLog(null, null, null, null, null, null);
			StringBuilder expLog = new StringBuilder();
			expLog.Append("USER=null\t");
			if (checkIP)
			{
				IPAddress ip = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
				expLog.Append(RMAuditLogger.Keys.Ip.ToString() + "=" + ip.GetHostAddress() + "\t"
					);
			}
			expLog.Append("OPERATION=null\tTARGET=null\tRESULT=SUCCESS");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), sLog);
		}

		/// <summary>
		/// Test the AuditLog format for successful events with the various
		/// parameters.
		/// </summary>
		private void TestSuccessLogFormat(bool checkIP)
		{
			TestSuccessLogFormatHelper(checkIP, null, null, null);
			TestSuccessLogFormatHelper(checkIP, Appid, null, null);
			TestSuccessLogFormatHelper(checkIP, null, null, Containerid);
			TestSuccessLogFormatHelper(checkIP, null, Attemptid, null);
			TestSuccessLogFormatHelper(checkIP, Appid, Attemptid, null);
			TestSuccessLogFormatHelper(checkIP, Appid, null, Containerid);
			TestSuccessLogFormatHelper(checkIP, null, Attemptid, Containerid);
			TestSuccessLogFormatHelper(checkIP, Appid, Attemptid, Containerid);
			TestSuccessLogNulls(checkIP);
		}

		/// <summary>Test the AuditLog format for failure events.</summary>
		private void TestFailureLogFormatHelper(bool checkIP, ApplicationId appId, ApplicationAttemptId
			 attemptId, ContainerId containerId)
		{
			string fLog = RMAuditLogger.CreateFailureLog(User, Operation, Perm, Target, Desc, 
				appId, attemptId, containerId);
			StringBuilder expLog = new StringBuilder();
			expLog.Append("USER=test\t");
			if (checkIP)
			{
				IPAddress ip = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
				expLog.Append(RMAuditLogger.Keys.Ip.ToString() + "=" + ip.GetHostAddress() + "\t"
					);
			}
			expLog.Append("OPERATION=oper\tTARGET=tgt\tRESULT=FAILURE\t");
			expLog.Append("DESCRIPTION=description of an audit log");
			expLog.Append("\tPERMISSIONS=admin group");
			if (appId != null)
			{
				expLog.Append("\tAPPID=app_1");
			}
			if (attemptId != null)
			{
				expLog.Append("\tAPPATTEMPTID=app_attempt_1");
			}
			if (containerId != null)
			{
				expLog.Append("\tCONTAINERID=container_1");
			}
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), fLog);
		}

		/// <summary>
		/// Test the AuditLog format for failure events with the various
		/// parameters.
		/// </summary>
		private void TestFailureLogFormat(bool checkIP)
		{
			TestFailureLogFormatHelper(checkIP, null, null, null);
			TestFailureLogFormatHelper(checkIP, Appid, null, null);
			TestFailureLogFormatHelper(checkIP, null, null, Containerid);
			TestFailureLogFormatHelper(checkIP, null, Attemptid, null);
			TestFailureLogFormatHelper(checkIP, Appid, Attemptid, null);
			TestFailureLogFormatHelper(checkIP, Appid, null, Containerid);
			TestFailureLogFormatHelper(checkIP, null, Attemptid, Containerid);
			TestFailureLogFormatHelper(checkIP, Appid, Attemptid, Containerid);
		}

		/// <summary>
		/// Test
		/// <see cref="RMAuditLogger"/>
		/// without IP set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAuditLoggerWithoutIP()
		{
			// test without ip
			TestSuccessLogFormat(false);
			TestFailureLogFormat(false);
		}

		/// <summary>
		/// A special extension of
		/// <see cref="Org.Apache.Hadoop.Ipc.TestRPC.TestImpl"/>
		/// RPC server with
		/// <see cref="Org.Apache.Hadoop.Ipc.TestRPC.TestImpl.Ping()"/>
		/// testing the audit logs.
		/// </summary>
		private class MyTestRPCServer : TestRPC.TestImpl
		{
			public override void Ping()
			{
				// test with ip set
				this._enclosing.TestSuccessLogFormat(true);
				this._enclosing.TestFailureLogFormat(true);
			}

			internal MyTestRPCServer(TestRMAuditLogger _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMAuditLogger _enclosing;
		}

		/// <summary>
		/// Test
		/// <see cref="RMAuditLogger"/>
		/// with IP set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAuditLoggerWithIP()
		{
			Configuration conf = new Configuration();
			// start the IPC server
			Org.Apache.Hadoop.Ipc.Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol
				)).SetInstance(new TestRMAuditLogger.MyTestRPCServer(this)).SetBindAddress("0.0.0.0"
				).SetPort(0).SetNumHandlers(5).SetVerbose(true).Build();
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			// Make a client connection and test the audit log
			TestRPC.TestProtocol proxy = (TestRPC.TestProtocol)RPC.GetProxy<TestRPC.TestProtocol
				>(TestRPC.TestProtocol.versionID, addr, conf);
			// Start the testcase
			proxy.Ping();
			server.Stop();
		}
	}
}
