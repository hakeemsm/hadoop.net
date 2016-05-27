using System;
using System.IO;
using System.Net;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Unit test for supporting method-name based compatible RPCs.</summary>
	public class TestRPCCompatibility
	{
		private const string Address = "0.0.0.0";

		private static IPEndPoint addr;

		private static RPC.Server server;

		private ProtocolProxy<object> proxy;

		public static readonly Log Log = LogFactory.GetLog(typeof(TestRPCCompatibility));

		private static Configuration conf = new Configuration();

		public abstract class TestProtocol0 : VersionedProtocol
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract void Ping();
		}

		public static class TestProtocol0Constants
		{
		}

		public interface TestProtocol1 : TestRPCCompatibility.TestProtocol0
		{
			/// <exception cref="System.IO.IOException"/>
			string Echo(string value);
		}

		public interface TestProtocol2 : TestRPCCompatibility.TestProtocol1
		{
			// TestProtocol2 is a compatible impl of TestProtocol1 - hence use its name
			/// <exception cref="System.IO.IOException"/>
			int Echo(int value);
		}

		public class TestImpl0 : TestRPCCompatibility.TestProtocol0
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHashCode)
			{
				Type inter;
				try
				{
					inter = (Type)GetType().GetGenericInterfaces()[0];
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				return ProtocolSignature.GetProtocolSignature(clientMethodsHashCode, GetProtocolVersion
					(protocol, clientVersion), inter);
			}

			public override void Ping()
			{
				return;
			}
		}

		public class TestImpl1 : TestRPCCompatibility.TestImpl0, TestRPCCompatibility.TestProtocol1
		{
			public virtual string Echo(string value)
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestRPCCompatibility.TestProtocol1.versionID;
			}
		}

		public class TestImpl2 : TestRPCCompatibility.TestImpl1, TestRPCCompatibility.TestProtocol2
		{
			public virtual int Echo(int value)
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestRPCCompatibility.TestProtocol2.versionID;
			}
		}

		[SetUp]
		public virtual void SetUp()
		{
			ProtocolSignature.ResetCache();
		}

		[TearDown]
		public virtual void TearDown()
		{
			if (proxy != null)
			{
				RPC.StopProxy(proxy.GetProxy());
				proxy = null;
			}
			if (server != null)
			{
				server.Stop();
				server = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVersion0ClientVersion1Server()
		{
			// old client vs new server
			// create a server with two handlers
			TestRPCCompatibility.TestImpl1 impl = new TestRPCCompatibility.TestImpl1();
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol1
				)).SetInstance(impl).SetBindAddress(Address).SetPort(0).SetNumHandlers(2).SetVerbose
				(false).Build();
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestRPCCompatibility.TestProtocol0
				), impl);
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
			proxy = RPC.GetProtocolProxy<TestRPCCompatibility.TestProtocol0>(TestRPCCompatibility.TestProtocol0
				.versionID, addr, conf);
			TestRPCCompatibility.TestProtocol0 proxy0 = (TestRPCCompatibility.TestProtocol0)proxy
				.GetProxy();
			proxy0.Ping();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVersion1ClientVersion0Server()
		{
			// old client vs new server
			// create a server with two handlers
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol0
				)).SetInstance(new TestRPCCompatibility.TestImpl0()).SetBindAddress(Address).SetPort
				(0).SetNumHandlers(2).SetVerbose(false).Build();
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
			proxy = RPC.GetProtocolProxy<TestRPCCompatibility.TestProtocol1>(TestRPCCompatibility.TestProtocol1
				.versionID, addr, conf);
			TestRPCCompatibility.TestProtocol1 proxy1 = (TestRPCCompatibility.TestProtocol1)proxy
				.GetProxy();
			proxy1.Ping();
			try
			{
				proxy1.Echo("hello");
				NUnit.Framework.Assert.Fail("Echo should fail");
			}
			catch (IOException)
			{
			}
		}

		private class Version2Client
		{
			private TestRPCCompatibility.TestProtocol2 proxy2;

			private ProtocolProxy<TestRPCCompatibility.TestProtocol2> serverInfo;

			/// <exception cref="System.IO.IOException"/>
			private Version2Client(TestRPCCompatibility _enclosing)
			{
				this._enclosing = _enclosing;
				this.serverInfo = RPC.GetProtocolProxy<TestRPCCompatibility.TestProtocol2>(TestRPCCompatibility.TestProtocol2
					.versionID, TestRPCCompatibility.addr, TestRPCCompatibility.conf);
				this.proxy2 = this.serverInfo.GetProxy();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.FormatException"/>
			public virtual int Echo(int value)
			{
				if (this.serverInfo.IsMethodSupported("echo", typeof(int)))
				{
					System.Console.Out.WriteLine("echo int is supported");
					return -value;
				}
				else
				{
					// use version 3 echo long
					// server is version 2
					System.Console.Out.WriteLine("echo int is NOT supported");
					return System.Convert.ToInt32(this.proxy2.Echo(value.ToString()));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string Echo(string value)
			{
				return this.proxy2.Echo(value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Ping()
			{
				this.proxy2.Ping();
			}

			private readonly TestRPCCompatibility _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVersion2ClientVersion1Server()
		{
			// Compatible new client & old server
			// create a server with two handlers
			TestRPCCompatibility.TestImpl1 impl = new TestRPCCompatibility.TestImpl1();
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol1
				)).SetInstance(impl).SetBindAddress(Address).SetPort(0).SetNumHandlers(2).SetVerbose
				(false).Build();
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestRPCCompatibility.TestProtocol0
				), impl);
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
			TestRPCCompatibility.Version2Client client = new TestRPCCompatibility.Version2Client
				(this);
			client.Ping();
			NUnit.Framework.Assert.AreEqual("hello", client.Echo("hello"));
			// echo(int) is not supported by server, so returning 3
			// This verifies that echo(int) and echo(String)'s hash codes are different
			NUnit.Framework.Assert.AreEqual(3, client.Echo(3));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVersion2ClientVersion2Server()
		{
			// equal version client and server
			// create a server with two handlers
			TestRPCCompatibility.TestImpl2 impl = new TestRPCCompatibility.TestImpl2();
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol2
				)).SetInstance(impl).SetBindAddress(Address).SetPort(0).SetNumHandlers(2).SetVerbose
				(false).Build();
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestRPCCompatibility.TestProtocol0
				), impl);
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
			TestRPCCompatibility.Version2Client client = new TestRPCCompatibility.Version2Client
				(this);
			client.Ping();
			NUnit.Framework.Assert.AreEqual("hello", client.Echo("hello"));
			// now that echo(int) is supported by the server, echo(int) should return -3
			NUnit.Framework.Assert.AreEqual(-3, client.Echo(3));
		}

		public interface TestProtocol3
		{
			int Echo(string value);

			int Echo(int value);

			int Echo_alias(int value);

			int Echo(int value1, int value2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHashCode()
		{
			// make sure that overriding methods have different hashcodes
			MethodInfo strMethod = typeof(TestRPCCompatibility.TestProtocol3).GetMethod("echo"
				, typeof(string));
			int stringEchoHash = ProtocolSignature.GetFingerprint(strMethod);
			MethodInfo intMethod = typeof(TestRPCCompatibility.TestProtocol3).GetMethod("echo"
				, typeof(int));
			int intEchoHash = ProtocolSignature.GetFingerprint(intMethod);
			NUnit.Framework.Assert.IsFalse(stringEchoHash == intEchoHash);
			// make sure methods with the same signature 
			// from different declaring classes have the same hash code
			int intEchoHash1 = ProtocolSignature.GetFingerprint(typeof(TestRPCCompatibility.TestProtocol2
				).GetMethod("echo", typeof(int)));
			NUnit.Framework.Assert.AreEqual(intEchoHash, intEchoHash1);
			// Methods with the same name and parameter types but different returning
			// types have different hash codes
			int stringEchoHash1 = ProtocolSignature.GetFingerprint(typeof(TestRPCCompatibility.TestProtocol2
				).GetMethod("echo", typeof(string)));
			NUnit.Framework.Assert.IsFalse(stringEchoHash == stringEchoHash1);
			// Make sure that methods with the same returning type and parameter types
			// but different method names have different hash code
			int intEchoHashAlias = ProtocolSignature.GetFingerprint(typeof(TestRPCCompatibility.TestProtocol3
				).GetMethod("echo_alias", typeof(int)));
			NUnit.Framework.Assert.IsFalse(intEchoHash == intEchoHashAlias);
			// Make sure that methods with the same returning type and method name but
			// larger number of parameter types have different hash code
			int intEchoHash2 = ProtocolSignature.GetFingerprint(typeof(TestRPCCompatibility.TestProtocol3
				).GetMethod("echo", typeof(int), typeof(int)));
			NUnit.Framework.Assert.IsFalse(intEchoHash == intEchoHash2);
			// make sure that methods order does not matter for method array hash code
			int hash1 = ProtocolSignature.GetFingerprint(new MethodInfo[] { intMethod, strMethod
				 });
			int hash2 = ProtocolSignature.GetFingerprint(new MethodInfo[] { strMethod, intMethod
				 });
			NUnit.Framework.Assert.AreEqual(hash1, hash2);
		}

		public abstract class TestProtocol4 : TestRPCCompatibility.TestProtocol2
		{
			public const long versionID = 4L;

			/// <exception cref="System.IO.IOException"/>
			public abstract int Echo(int value);
		}

		public static class TestProtocol4Constants
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestVersionMismatch()
		{
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol2
				)).SetInstance(new TestRPCCompatibility.TestImpl2()).SetBindAddress(Address).SetPort
				(0).SetNumHandlers(2).SetVerbose(false).Build();
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
			TestRPCCompatibility.TestProtocol4 proxy = RPC.GetProxy<TestRPCCompatibility.TestProtocol4
				>(TestRPCCompatibility.TestProtocol4.versionID, addr, conf);
			try
			{
				proxy.Echo(21);
				NUnit.Framework.Assert.Fail("The call must throw VersionMismatch exception");
			}
			catch (RemoteException ex)
			{
				NUnit.Framework.Assert.AreEqual(typeof(RPC.VersionMismatch).FullName, ex.GetClassName
					());
				NUnit.Framework.Assert.IsTrue(ex.GetErrorCode().Equals(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ErrorRpcVersionMismatch));
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.Fail("Expected version mismatch but got " + ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestIsMethodSupported()
		{
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol2
				)).SetInstance(new TestRPCCompatibility.TestImpl2()).SetBindAddress(Address).SetPort
				(0).SetNumHandlers(2).SetVerbose(false).Build();
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
			TestRPCCompatibility.TestProtocol2 proxy = RPC.GetProxy<TestRPCCompatibility.TestProtocol2
				>(TestRPCCompatibility.TestProtocol2.versionID, addr, conf);
			bool supported = RpcClientUtil.IsMethodSupported(proxy, typeof(TestRPCCompatibility.TestProtocol2
				), RPC.RpcKind.RpcWritable, RPC.GetProtocolVersion(typeof(TestRPCCompatibility.TestProtocol2
				)), "echo");
			NUnit.Framework.Assert.IsTrue(supported);
			supported = RpcClientUtil.IsMethodSupported(proxy, typeof(TestRPCCompatibility.TestProtocol2
				), RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(TestRPCCompatibility.TestProtocol2
				)), "echo");
			NUnit.Framework.Assert.IsFalse(supported);
		}

		/// <summary>
		/// Verify that ProtocolMetaInfoServerSideTranslatorPB correctly looks up
		/// the server registry to extract protocol signatures and versions.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProtocolMetaInfoSSTranslatorPB()
		{
			TestRPCCompatibility.TestImpl1 impl = new TestRPCCompatibility.TestImpl1();
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPCCompatibility.TestProtocol1
				)).SetInstance(impl).SetBindAddress(Address).SetPort(0).SetNumHandlers(2).SetVerbose
				(false).Build();
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestRPCCompatibility.TestProtocol0
				), impl);
			server.Start();
			ProtocolMetaInfoServerSideTranslatorPB xlator = new ProtocolMetaInfoServerSideTranslatorPB
				(server);
			ProtocolInfoProtos.GetProtocolSignatureResponseProto resp = xlator.GetProtocolSignature
				(null, CreateGetProtocolSigRequestProto(typeof(TestRPCCompatibility.TestProtocol1
				), RPC.RpcKind.RpcProtocolBuffer));
			//No signatures should be found
			NUnit.Framework.Assert.AreEqual(0, resp.GetProtocolSignatureCount());
			resp = xlator.GetProtocolSignature(null, CreateGetProtocolSigRequestProto(typeof(
				TestRPCCompatibility.TestProtocol1), RPC.RpcKind.RpcWritable));
			NUnit.Framework.Assert.AreEqual(1, resp.GetProtocolSignatureCount());
			ProtocolInfoProtos.ProtocolSignatureProto sig = resp.GetProtocolSignatureList()[0
				];
			NUnit.Framework.Assert.AreEqual(TestRPCCompatibility.TestProtocol1.versionID, sig
				.GetVersion());
			bool found = false;
			int expected = ProtocolSignature.GetFingerprint(typeof(TestRPCCompatibility.TestProtocol1
				).GetMethod("echo", typeof(string)));
			foreach (int m in sig.GetMethodsList())
			{
				if (expected == m)
				{
					found = true;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue(found);
		}

		private ProtocolInfoProtos.GetProtocolSignatureRequestProto CreateGetProtocolSigRequestProto
			(Type protocol, RPC.RpcKind rpcKind)
		{
			ProtocolInfoProtos.GetProtocolSignatureRequestProto.Builder builder = ProtocolInfoProtos.GetProtocolSignatureRequestProto
				.NewBuilder();
			builder.SetProtocol(protocol.FullName);
			builder.SetRpcKind(rpcKind.ToString());
			return ((ProtocolInfoProtos.GetProtocolSignatureRequestProto)builder.Build());
		}
	}
}
