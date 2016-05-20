using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// This class maintains a cache of protocol versions and corresponding protocol
	/// signatures, keyed by server address, protocol and rpc kind.
	/// </summary>
	/// <remarks>
	/// This class maintains a cache of protocol versions and corresponding protocol
	/// signatures, keyed by server address, protocol and rpc kind.
	/// The cache is lazily populated.
	/// </remarks>
	public class RpcClientUtil
	{
		private static com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private const int PRIME = 16777619;

		private class ProtoSigCacheKey
		{
			private java.net.InetSocketAddress serverAddress;

			private string protocol;

			private string rpcKind;

			internal ProtoSigCacheKey(java.net.InetSocketAddress addr, string p, string rk)
			{
				this.serverAddress = addr;
				this.protocol = p;
				this.rpcKind = rk;
			}

			public override int GetHashCode()
			{
				//Object
				int result = 1;
				result = PRIME * result + ((serverAddress == null) ? 0 : serverAddress.GetHashCode
					());
				result = PRIME * result + ((protocol == null) ? 0 : protocol.GetHashCode());
				result = PRIME * result + ((rpcKind == null) ? 0 : rpcKind.GetHashCode());
				return result;
			}

			public override bool Equals(object other)
			{
				//Object
				if (other == this)
				{
					return true;
				}
				if (other is org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey)
				{
					org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey otherKey = (org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey
						)other;
					return (serverAddress.Equals(otherKey.serverAddress) && protocol.Equals(otherKey.
						protocol) && rpcKind.Equals(otherKey.rpcKind));
				}
				return false;
			}
		}

		private static java.util.concurrent.ConcurrentHashMap<org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey
			, System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
			>> signatureMap = new java.util.concurrent.ConcurrentHashMap<org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey
			, System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
			>>();

		private static void putVersionSignatureMap(java.net.InetSocketAddress addr, string
			 protocol, string rpcKind, System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
			> map)
		{
			signatureMap[new org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey(addr, protocol
				, rpcKind)] = map;
		}

		private static System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
			> getVersionSignatureMap(java.net.InetSocketAddress addr, string protocol, string
			 rpcKind)
		{
			return signatureMap[new org.apache.hadoop.ipc.RpcClientUtil.ProtoSigCacheKey(addr
				, protocol, rpcKind)];
		}

		/// <summary>Returns whether the given method is supported or not.</summary>
		/// <remarks>
		/// Returns whether the given method is supported or not.
		/// The protocol signatures are fetched and cached. The connection id for the
		/// proxy provided is re-used.
		/// </remarks>
		/// <param name="rpcProxy">Proxy which provides an existing connection id.</param>
		/// <param name="protocol">Protocol for which the method check is required.</param>
		/// <param name="rpcKind">The RpcKind for which the method check is required.</param>
		/// <param name="version">The version at the client.</param>
		/// <param name="methodName">Name of the method.</param>
		/// <returns>true if the method is supported, false otherwise.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool isMethodSupported(object rpcProxy, java.lang.Class protocol, org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, long version, string methodName)
		{
			java.net.InetSocketAddress serverAddress = org.apache.hadoop.ipc.RPC.getServerAddress
				(rpcProxy);
			System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
				> versionMap = getVersionSignatureMap(serverAddress, protocol.getName(), rpcKind
				.ToString());
			if (versionMap == null)
			{
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					();
				org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.ProtocolMetaInfoPB)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
				org.apache.hadoop.ipc.ProtocolMetaInfoPB protocolInfoProxy = getProtocolMetaInfoProxy
					(rpcProxy, conf);
				org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto.Builder
					 builder = org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto
					.newBuilder();
				builder.setProtocol(protocol.getName());
				builder.setRpcKind(rpcKind.ToString());
				org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto
					 resp;
				try
				{
					resp = protocolInfoProxy.getProtocolSignature(NULL_CONTROLLER, ((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto
						)builder.build()));
				}
				catch (com.google.protobuf.ServiceException se)
				{
					throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(se);
				}
				versionMap = convertProtocolSignatureProtos(resp.getProtocolSignatureList());
				putVersionSignatureMap(serverAddress, protocol.getName(), rpcKind.ToString(), versionMap
					);
			}
			// Assuming unique method names.
			java.lang.reflect.Method desiredMethod;
			java.lang.reflect.Method[] allMethods = protocol.getMethods();
			desiredMethod = null;
			foreach (java.lang.reflect.Method m in allMethods)
			{
				if (m.getName().Equals(methodName))
				{
					desiredMethod = m;
					break;
				}
			}
			if (desiredMethod == null)
			{
				return false;
			}
			int methodHash = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(desiredMethod
				);
			return methodExists(methodHash, version, versionMap);
		}

		private static System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
			> convertProtocolSignatureProtos(System.Collections.Generic.IList<org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto
			> protoList)
		{
			System.Collections.Generic.IDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
				> map = new System.Collections.Generic.SortedDictionary<long, org.apache.hadoop.ipc.ProtocolSignature
				>();
			foreach (org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto
				 p in protoList)
			{
				int[] methods = new int[p.getMethodsList().Count];
				int index = 0;
				foreach (int m in p.getMethodsList())
				{
					methods[index++] = m;
				}
				map[p.getVersion()] = new org.apache.hadoop.ipc.ProtocolSignature(p.getVersion(), 
					methods);
			}
			return map;
		}

		private static bool methodExists(int methodHash, long version, System.Collections.Generic.IDictionary
			<long, org.apache.hadoop.ipc.ProtocolSignature> versionMap)
		{
			org.apache.hadoop.ipc.ProtocolSignature sig = versionMap[version];
			if (sig != null)
			{
				foreach (int m in sig.getMethods())
				{
					if (m == methodHash)
					{
						return true;
					}
				}
			}
			return false;
		}

		// The proxy returned re-uses the underlying connection. This is a special 
		// mechanism for ProtocolMetaInfoPB.
		// Don't do this for any other protocol, it might cause a security hole.
		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.ipc.ProtocolMetaInfoPB getProtocolMetaInfoProxy(
			object proxy, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.ipc.RpcInvocationHandler inv = (org.apache.hadoop.ipc.RpcInvocationHandler
				)java.lang.reflect.Proxy.getInvocationHandler(proxy);
			return org.apache.hadoop.ipc.RPC.getProtocolEngine(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtocolMetaInfoPB)), conf).getProtocolMetaInfoProxy
				(inv.getConnectionId(), conf, org.apache.hadoop.net.NetUtils.getDefaultSocketFactory
				(conf)).getProxy();
		}

		/// <summary>Convert an RPC method to a string.</summary>
		/// <remarks>
		/// Convert an RPC method to a string.
		/// The format we want is 'MethodOuterClassShortName#methodName'.
		/// For example, if the method is:
		/// org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.
		/// ClientNamenodeProtocol.BlockingInterface.getServerDefaults
		/// the format we want is:
		/// ClientNamenodeProtocol#getServerDefaults
		/// </remarks>
		public static string methodToTraceString(java.lang.reflect.Method method)
		{
			java.lang.Class clazz = method.getDeclaringClass();
			while (true)
			{
				java.lang.Class next = clazz.getEnclosingClass();
				if (next == null || next.getEnclosingClass() == null)
				{
					break;
				}
				clazz = next;
			}
			return clazz.getSimpleName() + "#" + method.getName();
		}
	}
}
