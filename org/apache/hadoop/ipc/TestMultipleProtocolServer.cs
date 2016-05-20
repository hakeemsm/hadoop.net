using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class TestMultipleProtocolServer
	{
		private const string ADDRESS = "0.0.0.0";

		private static java.net.InetSocketAddress addr;

		private static org.apache.hadoop.ipc.RPC.Server server;

		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		internal abstract class Foo0 : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string ping();
		}

		internal static class Foo0Constants
		{
		}

		internal abstract class Foo1 : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 1L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string ping();

			/// <exception cref="System.IO.IOException"/>
			public abstract string ping2();
		}

		internal static class Foo1Constants
		{
		}

		internal abstract class FooUnimplemented : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 2L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string ping();
		}

		internal static class FooUnimplementedConstants
		{
		}

		internal abstract class Mixin : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract void hello();
		}

		internal static class MixinConstants
		{
		}

		internal abstract class Bar : org.apache.hadoop.ipc.TestMultipleProtocolServer.Mixin
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract int echo(int i);
		}

		internal static class BarConstants
		{
		}

		internal class Foo0Impl : org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long getProtocolVersion(string protocol, long clientVersion)
			{
				return org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
				 protocol, long clientVersion, int clientMethodsHash)
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
				return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
					, this.getProtocolVersion(protocol, clientVersion), inter);
			}

			public override string ping()
			{
				return "Foo0";
			}

			internal Foo0Impl(TestMultipleProtocolServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultipleProtocolServer _enclosing;
		}

		internal class Foo1Impl : org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long getProtocolVersion(string protocol, long clientVersion)
			{
				return org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
				 protocol, long clientVersion, int clientMethodsHash)
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
				return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
					, this.getProtocolVersion(protocol, clientVersion), inter);
			}

			public override string ping()
			{
				return "Foo1";
			}

			public override string ping2()
			{
				return "Foo1";
			}

			internal Foo1Impl(TestMultipleProtocolServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultipleProtocolServer _enclosing;
		}

		internal class BarImpl : org.apache.hadoop.ipc.TestMultipleProtocolServer.Bar
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long getProtocolVersion(string protocol, long clientVersion)
			{
				return org.apache.hadoop.ipc.TestMultipleProtocolServer.Bar.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
				 protocol, long clientVersion, int clientMethodsHash)
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
				return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
					, this.getProtocolVersion(protocol, clientVersion), inter);
			}

			public override int echo(int i)
			{
				return i;
			}

			public virtual void hello()
			{
			}

			internal BarImpl(TestMultipleProtocolServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultipleProtocolServer _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			// create a server with two handlers
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0))).setInstance(new 
				org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0Impl(this)).setBindAddress(
				ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false).build();
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1)), new org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1Impl
				(this));
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.Bar)), new org.apache.hadoop.ipc.TestMultipleProtocolServer.BarImpl
				(this));
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.Mixin)), new org.apache.hadoop.ipc.TestMultipleProtocolServer.BarImpl
				(this));
			// Add Protobuf server
			// Create server side implementation
			org.apache.hadoop.ipc.TestProtoBufRpc.PBServerImpl pbServerImpl = new org.apache.hadoop.ipc.TestProtoBufRpc.PBServerImpl
				();
			com.google.protobuf.BlockingService service = org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto
				.newReflectiveBlockingService(pbServerImpl);
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService)), service);
			server.start();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			server.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void test1()
		{
			org.apache.hadoop.ipc.ProtocolProxy<object> proxy;
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0
				>(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0.versionID, addr, conf);
			org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0 foo0 = (org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0
				)proxy.getProxy();
			NUnit.Framework.Assert.AreEqual("Foo0", foo0.ping());
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1
				>(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1.versionID, addr, conf);
			org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1 foo1 = (org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1
				)proxy.getProxy();
			NUnit.Framework.Assert.AreEqual("Foo1", foo1.ping());
			NUnit.Framework.Assert.AreEqual("Foo1", foo1.ping());
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestMultipleProtocolServer.Bar
				>(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1.versionID, addr, conf);
			org.apache.hadoop.ipc.TestMultipleProtocolServer.Bar bar = (org.apache.hadoop.ipc.TestMultipleProtocolServer.Bar
				)proxy.getProxy();
			NUnit.Framework.Assert.AreEqual(99, bar.echo(99));
			// Now test Mixin class method
			org.apache.hadoop.ipc.TestMultipleProtocolServer.Mixin mixin = bar;
			mixin.hello();
		}

		// Server does not implement the FooUnimplemented version of protocol Foo.
		// See that calls to it fail.
		/// <exception cref="System.IO.IOException"/>
		public virtual void testNonExistingProtocol()
		{
			org.apache.hadoop.ipc.ProtocolProxy<object> proxy;
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented
				>(org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented.versionID, addr
				, conf);
			org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented foo = (org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented
				)proxy.getProxy();
			foo.ping();
		}

		/// <summary>
		/// getProtocolVersion of an unimplemented version should return highest version
		/// Similarly getProtocolSignature should work.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testNonExistingProtocol2()
		{
			org.apache.hadoop.ipc.ProtocolProxy<object> proxy;
			proxy = org.apache.hadoop.ipc.RPC.getProtocolProxy<org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented
				>(org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented.versionID, addr
				, conf);
			org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented foo = (org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented
				)proxy.getProxy();
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1
				.versionID, foo.getProtocolVersion(org.apache.hadoop.ipc.RPC.getProtocolName(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented))), org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented
				.versionID));
			foo.getProtocolSignature(org.apache.hadoop.ipc.RPC.getProtocolName(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented))), org.apache.hadoop.ipc.TestMultipleProtocolServer.FooUnimplemented
				.versionID, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIncorrectServerCreation()
		{
			new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo1))).setInstance(new 
				org.apache.hadoop.ipc.TestMultipleProtocolServer.Foo0Impl(this)).setBindAddress(
				ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false).build();
		}

		// Now test a PB service - a server  hosts both PB and Writable Rpcs.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPBService()
		{
			// Set RPC engine to protobuf RPC engine
			org.apache.hadoop.conf.Configuration conf2 = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf2, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService client = org.apache.hadoop.ipc.RPC
				.getProxy<org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService>(0, addr, conf2);
			org.apache.hadoop.ipc.TestProtoBufRpc.testProtoBufRpc(client);
		}
	}
}
