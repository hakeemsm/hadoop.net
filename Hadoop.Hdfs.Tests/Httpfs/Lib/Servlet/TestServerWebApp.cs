using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Lib.Server;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	public class TestServerWebApp : HTestCase
	{
		public virtual void GetHomeDirNotDef()
		{
			ServerWebApp.GetHomeDir("TestServerWebApp00");
		}

		[NUnit.Framework.Test]
		public virtual void GetHomeDir()
		{
			Runtime.SetProperty("TestServerWebApp0.home.dir", "/tmp");
			NUnit.Framework.Assert.AreEqual(ServerWebApp.GetHomeDir("TestServerWebApp0"), "/tmp"
				);
			NUnit.Framework.Assert.AreEqual(ServerWebApp.GetDir("TestServerWebApp0", ".log.dir"
				, "/tmp/log"), "/tmp/log");
			Runtime.SetProperty("TestServerWebApp0.log.dir", "/tmplog");
			NUnit.Framework.Assert.AreEqual(ServerWebApp.GetDir("TestServerWebApp0", ".log.dir"
				, "/tmp/log"), "/tmplog");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Lifecycle()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Runtime.SetProperty("TestServerWebApp1.home.dir", dir);
			Runtime.SetProperty("TestServerWebApp1.config.dir", dir);
			Runtime.SetProperty("TestServerWebApp1.log.dir", dir);
			Runtime.SetProperty("TestServerWebApp1.temp.dir", dir);
			ServerWebApp server = new _ServerWebApp_56("TestServerWebApp1");
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Undef);
			server.ContextInitialized(null);
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Normal);
			server.ContextDestroyed(null);
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Shutdown);
		}

		private sealed class _ServerWebApp_56 : ServerWebApp
		{
			public _ServerWebApp_56(string baseArg1)
				: base(baseArg1)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void FailedInit()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Runtime.SetProperty("TestServerWebApp2.home.dir", dir);
			Runtime.SetProperty("TestServerWebApp2.config.dir", dir);
			Runtime.SetProperty("TestServerWebApp2.log.dir", dir);
			Runtime.SetProperty("TestServerWebApp2.temp.dir", dir);
			Runtime.SetProperty("testserverwebapp2.services", "FOO");
			ServerWebApp server = new _ServerWebApp_75("TestServerWebApp2");
			server.ContextInitialized(null);
		}

		private sealed class _ServerWebApp_75 : ServerWebApp
		{
			public _ServerWebApp_75(string baseArg1)
				: base(baseArg1)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void TestResolveAuthority()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Runtime.SetProperty("TestServerWebApp3.home.dir", dir);
			Runtime.SetProperty("TestServerWebApp3.config.dir", dir);
			Runtime.SetProperty("TestServerWebApp3.log.dir", dir);
			Runtime.SetProperty("TestServerWebApp3.temp.dir", dir);
			Runtime.SetProperty("testserverwebapp3.http.hostname", "localhost");
			Runtime.SetProperty("testserverwebapp3.http.port", "14000");
			ServerWebApp server = new _ServerWebApp_91("TestServerWebApp3");
			IPEndPoint address = server.ResolveAuthority();
			NUnit.Framework.Assert.AreEqual("localhost", address.GetHostName());
			NUnit.Framework.Assert.AreEqual(14000, address.Port);
		}

		private sealed class _ServerWebApp_91 : ServerWebApp
		{
			public _ServerWebApp_91(string baseArg1)
				: base(baseArg1)
			{
			}
		}
	}
}
