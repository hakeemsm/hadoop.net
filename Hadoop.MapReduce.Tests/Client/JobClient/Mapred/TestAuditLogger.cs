using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Tests
	/// <see cref="AuditLogger"/>
	/// .
	/// </summary>
	public class TestAuditLogger : TestCase
	{
		private const string User = "test";

		private const string Operation = "oper";

		private const string Target = "tgt";

		private const string Perm = "admin group";

		private const string Desc = "description of an audit log";

		/// <summary>Test the AuditLog format with key-val pair.</summary>
		public virtual void TestKeyValLogFormat()
		{
			StringBuilder actLog = new StringBuilder();
			StringBuilder expLog = new StringBuilder();
			// add the first k=v pair and check
			AuditLogger.Start(AuditLogger.Keys.User, User, actLog);
			expLog.Append("USER=test");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
			// append another k1=v1 pair to already added k=v and test
			AuditLogger.Add(AuditLogger.Keys.Operation, Operation, actLog);
			expLog.Append("\tOPERATION=oper");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
			// append another k1=null pair and test
			AuditLogger.Add(AuditLogger.Keys.Permissions, (string)null, actLog);
			expLog.Append("\tPERMISSIONS=null");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
			// now add the target and check of the final string
			AuditLogger.Add(AuditLogger.Keys.Target, Target, actLog);
			expLog.Append("\tTARGET=tgt");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), actLog.ToString());
		}

		/// <summary>Test the AuditLog format for successful events.</summary>
		private void TestSuccessLogFormat(bool checkIP)
		{
			// check without the IP
			string sLog = AuditLogger.CreateSuccessLog(User, Operation, Target);
			StringBuilder expLog = new StringBuilder();
			expLog.Append("USER=test\t");
			if (checkIP)
			{
				IPAddress ip = Server.GetRemoteIp();
				expLog.Append(AuditLogger.Keys.Ip.ToString() + "=" + ip.GetHostAddress() + "\t");
			}
			expLog.Append("OPERATION=oper\tTARGET=tgt\tRESULT=SUCCESS");
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), sLog);
		}

		/// <summary>Test the AuditLog format for failure events.</summary>
		private void TestFailureLogFormat(bool checkIP, string perm)
		{
			string fLog = AuditLogger.CreateFailureLog(User, Operation, perm, Target, Desc);
			StringBuilder expLog = new StringBuilder();
			expLog.Append("USER=test\t");
			if (checkIP)
			{
				IPAddress ip = Server.GetRemoteIp();
				expLog.Append(AuditLogger.Keys.Ip.ToString() + "=" + ip.GetHostAddress() + "\t");
			}
			expLog.Append("OPERATION=oper\tTARGET=tgt\tRESULT=FAILURE\t");
			expLog.Append("DESCRIPTION=description of an audit log\t");
			expLog.Append("PERMISSIONS=" + perm);
			NUnit.Framework.Assert.AreEqual(expLog.ToString(), fLog);
		}

		/// <summary>Test the AuditLog format for failure events.</summary>
		private void TestFailureLogFormat(bool checkIP)
		{
			TestFailureLogFormat(checkIP, Perm);
			TestFailureLogFormat(checkIP, null);
		}

		/// <summary>
		/// Test
		/// <see cref="AuditLogger"/>
		/// without IP set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAuditLoggerWithoutIP()
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

			internal MyTestRPCServer(TestAuditLogger _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAuditLogger _enclosing;
		}

		/// <summary>
		/// Test
		/// <see cref="AuditLogger"/>
		/// with IP set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAuditLoggerWithIP()
		{
			Configuration conf = new Configuration();
			// start the IPC server
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestAuditLogger.MyTestRPCServer(this)).SetBindAddress("0.0.0.0").SetPort(0)
				.Build();
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
