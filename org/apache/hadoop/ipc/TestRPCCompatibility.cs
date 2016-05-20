using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Unit test for supporting method-name based compatible RPCs.</summary>
	public class TestRPCCompatibility
	{
		private const string ADDRESS = "0.0.0.0";

		private static java.net.InetSocketAddress addr;

		private static org.apache.hadoop.ipc.RPC.Server server;

		private org.apache.hadoop.ipc.ProtocolProxy<object> proxy;

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPCCompatibility
			)));

		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		public abstract class TestProtocol0 : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract void ping();
		}

		public static class TestProtocol0Constants
		{
		}

		public interface TestProtocol1 : org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0
		{
			/// <exception cref="System.IO.IOException"/>
			string echo(string value);
		}

		public interface TestProtocol2 : org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1
		{
			// TestProtocol2 is a compatible impl of TestProtocol1 - hence use its name
			/// <exception cref="System.IO.IOException"/>
			int echo(int value);
		}

		public class TestImpl0 : org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long getProtocolVersion(string protocol, long clientVersion)
			{
				return versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
				 protocol, long clientVersion, int clientMethodsHashCode)
			{
				java.lang.Class inter;
				try
				{
					inter = (java.lang.Class)Sharpen.Runtime.getClassForObject(this).getGenericInterfaces
						()[0];
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException(e);
				}
				return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHashCode
					, getProtocolVersion(protocol, clientVersion), inter);
			}

			public override void ping()
			{
				return;
			}
		}

		public class TestImpl1 : org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl0, org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1
		{
			public virtual string echo(string value)
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long getProtocolVersion(string protocol, long clientVersion)
			{
				return org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1.versionID;
			}
		}

		public class TestImpl2 : org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1, org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2
		{
			public virtual int echo(int value)
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long getProtocolVersion(string protocol, long clientVersion)
			{
				return org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2.versionID;
			}
		}

		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			org.apache.hadoop.ipc.ProtocolSignature.resetCache();
		}

		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			if (proxy != null)
			{
				org.apache.hadoop.ipc.RPC.stopProxy(proxy.getProxy());
				proxy = null;
			}
			if (server != null)
			{
				server.stop();
				server = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVersion0ClientVersion1Server()
		{
			// old client vs new server
			// create a server with two handlers
			org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1 impl = new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1
				();
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1))).setInstance(
				impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false).build
				();
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0)), impl);
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0
				>(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0.versionID, addr, conf
				);
			org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0 proxy0 = (org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0
				)proxy.getProxy();
			proxy0.ping();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVersion1ClientVersion0Server()
		{
			// old client vs new server
			// create a server with two handlers
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0))).setInstance(
				new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl0()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(2).setVerbose(false).build();
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1
				>(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1.versionID, addr, conf
				);
			org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1 proxy1 = (org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1
				)proxy.getProxy();
			proxy1.ping();
			try
			{
				proxy1.echo("hello");
				NUnit.Framework.Assert.Fail("Echo should fail");
			}
			catch (System.IO.IOException)
			{
			}
		}

		private class Version2Client
		{
			private org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2 proxy2;

			private org.apache.hadoop.ipc.ProtocolProxy<org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2
				> serverInfo;

			/// <exception cref="System.IO.IOException"/>
			private Version2Client(TestRPCCompatibility _enclosing)
			{
				this._enclosing = _enclosing;
				this.serverInfo = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2
					>(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2.versionID, org.apache.hadoop.ipc.TestRPCCompatibility
					.addr, org.apache.hadoop.ipc.TestRPCCompatibility.conf);
				this.proxy2 = this.serverInfo.getProxy();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.lang.NumberFormatException"/>
			public virtual int echo(int value)
			{
				if (this.serverInfo.isMethodSupported("echo", Sharpen.Runtime.getClassForType(typeof(
					int))))
				{
					System.Console.Out.WriteLine("echo int is supported");
					return -value;
				}
				else
				{
					// use version 3 echo long
					// server is version 2
					System.Console.Out.WriteLine("echo int is NOT supported");
					return System.Convert.ToInt32(this.proxy2.echo(Sharpen.Runtime.getStringValueOf(value
						)));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string echo(string value)
			{
				return this.proxy2.echo(value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ping()
			{
				this.proxy2.ping();
			}

			private readonly TestRPCCompatibility _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVersion2ClientVersion1Server()
		{
			// Compatible new client & old server
			// create a server with two handlers
			org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1 impl = new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1
				();
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1))).setInstance(
				impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false).build
				();
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0)), impl);
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			org.apache.hadoop.ipc.TestRPCCompatibility.Version2Client client = new org.apache.hadoop.ipc.TestRPCCompatibility.Version2Client
				(this);
			client.ping();
			NUnit.Framework.Assert.AreEqual("hello", client.echo("hello"));
			// echo(int) is not supported by server, so returning 3
			// This verifies that echo(int) and echo(String)'s hash codes are different
			NUnit.Framework.Assert.AreEqual(3, client.echo(3));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVersion2ClientVersion2Server()
		{
			// equal version client and server
			// create a server with two handlers
			org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl2 impl = new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl2
				();
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2))).setInstance(
				impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false).build
				();
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0)), impl);
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			org.apache.hadoop.ipc.TestRPCCompatibility.Version2Client client = new org.apache.hadoop.ipc.TestRPCCompatibility.Version2Client
				(this);
			client.ping();
			NUnit.Framework.Assert.AreEqual("hello", client.echo("hello"));
			// now that echo(int) is supported by the server, echo(int) should return -3
			NUnit.Framework.Assert.AreEqual(-3, client.echo(3));
		}

		public interface TestProtocol3
		{
			int echo(string value);

			int echo(int value);

			int echo_alias(int value);

			int echo(int value1, int value2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testHashCode()
		{
			// make sure that overriding methods have different hashcodes
			java.lang.reflect.Method strMethod = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol3
				)).getMethod("echo", Sharpen.Runtime.getClassForType(typeof(string)));
			int stringEchoHash = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(strMethod
				);
			java.lang.reflect.Method intMethod = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol3
				)).getMethod("echo", Sharpen.Runtime.getClassForType(typeof(int)));
			int intEchoHash = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(intMethod
				);
			NUnit.Framework.Assert.IsFalse(stringEchoHash == intEchoHash);
			// make sure methods with the same signature 
			// from different declaring classes have the same hash code
			int intEchoHash1 = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2)).getMethod("echo"
				, Sharpen.Runtime.getClassForType(typeof(int))));
			NUnit.Framework.Assert.AreEqual(intEchoHash, intEchoHash1);
			// Methods with the same name and parameter types but different returning
			// types have different hash codes
			int stringEchoHash1 = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2)).getMethod("echo"
				, Sharpen.Runtime.getClassForType(typeof(string))));
			NUnit.Framework.Assert.IsFalse(stringEchoHash == stringEchoHash1);
			// Make sure that methods with the same returning type and parameter types
			// but different method names have different hash code
			int intEchoHashAlias = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol3)).getMethod("echo_alias"
				, Sharpen.Runtime.getClassForType(typeof(int))));
			NUnit.Framework.Assert.IsFalse(intEchoHash == intEchoHashAlias);
			// Make sure that methods with the same returning type and method name but
			// larger number of parameter types have different hash code
			int intEchoHash2 = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol3)).getMethod("echo"
				, Sharpen.Runtime.getClassForType(typeof(int)), Sharpen.Runtime.getClassForType(
				typeof(int))));
			NUnit.Framework.Assert.IsFalse(intEchoHash == intEchoHash2);
			// make sure that methods order does not matter for method array hash code
			int hash1 = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(new java.lang.reflect.Method
				[] { intMethod, strMethod });
			int hash2 = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(new java.lang.reflect.Method
				[] { strMethod, intMethod });
			NUnit.Framework.Assert.AreEqual(hash1, hash2);
		}

		public abstract class TestProtocol4 : org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2
		{
			public const long versionID = 4L;

			/// <exception cref="System.IO.IOException"/>
			public abstract int echo(int value);
		}

		public static class TestProtocol4Constants
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testVersionMismatch()
		{
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2))).setInstance(
				new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl2()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(2).setVerbose(false).build();
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol4 proxy = org.apache.hadoop.ipc.RPC
				.getProxy<org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol4>(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol4
				.versionID, addr, conf);
			try
			{
				proxy.echo(21);
				NUnit.Framework.Assert.Fail("The call must throw VersionMismatch exception");
			}
			catch (org.apache.hadoop.ipc.RemoteException ex)
			{
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RPC.VersionMismatch
					)).getName(), ex.getClassName());
				NUnit.Framework.Assert.IsTrue(ex.getErrorCode().Equals(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ERROR_RPC_VERSION_MISMATCH));
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.Fail("Expected version mismatch but got " + ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testIsMethodSupported()
		{
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2))).setInstance(
				new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl2()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(2).setVerbose(false).build();
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2 proxy = org.apache.hadoop.ipc.RPC
				.getProxy<org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2>(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2
				.versionID, addr, conf);
			bool supported = org.apache.hadoop.ipc.RpcClientUtil.isMethodSupported(proxy, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_WRITABLE, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2))), "echo");
			NUnit.Framework.Assert.IsTrue(supported);
			supported = org.apache.hadoop.ipc.RpcClientUtil.isMethodSupported(proxy, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol2))), "echo");
			NUnit.Framework.Assert.IsFalse(supported);
		}

		/// <summary>
		/// Verify that ProtocolMetaInfoServerSideTranslatorPB correctly looks up
		/// the server registry to extract protocol signatures and versions.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testProtocolMetaInfoSSTranslatorPB()
		{
			org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1 impl = new org.apache.hadoop.ipc.TestRPCCompatibility.TestImpl1
				();
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1))).setInstance(
				impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false).build
				();
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol0)), impl);
			server.start();
			org.apache.hadoop.ipc.ProtocolMetaInfoServerSideTranslatorPB xlator = new org.apache.hadoop.ipc.ProtocolMetaInfoServerSideTranslatorPB
				(server);
			org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto
				 resp = xlator.getProtocolSignature(null, createGetProtocolSigRequestProto(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER));
			//No signatures should be found
			NUnit.Framework.Assert.AreEqual(0, resp.getProtocolSignatureCount());
			resp = xlator.getProtocolSignature(null, createGetProtocolSigRequestProto(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_WRITABLE));
			NUnit.Framework.Assert.AreEqual(1, resp.getProtocolSignatureCount());
			org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto sig = resp
				.getProtocolSignatureList()[0];
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1
				.versionID, sig.getVersion());
			bool found = false;
			int expected = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPCCompatibility.TestProtocol1)).getMethod("echo"
				, Sharpen.Runtime.getClassForType(typeof(string))));
			foreach (int m in sig.getMethodsList())
			{
				if (expected == m)
				{
					found = true;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue(found);
		}

		private org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto
			 createGetProtocolSigRequestProto(java.lang.Class protocol, org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind)
		{
			org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto.Builder
				 builder = org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto
				.newBuilder();
			builder.setProtocol(protocol.getName());
			builder.setRpcKind(rpcKind.ToString());
			return ((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto
				)builder.build());
		}
	}
}
