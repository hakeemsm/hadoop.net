using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Ipc
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
		private static RpcController NullController = null;

		private const int Prime = 16777619;

		private class ProtoSigCacheKey
		{
			private IPEndPoint serverAddress;

			private string protocol;

			private string rpcKind;

			internal ProtoSigCacheKey(IPEndPoint addr, string p, string rk)
			{
				this.serverAddress = addr;
				this.protocol = p;
				this.rpcKind = rk;
			}

			public override int GetHashCode()
			{
				//Object
				int result = 1;
				result = Prime * result + ((serverAddress == null) ? 0 : serverAddress.GetHashCode
					());
				result = Prime * result + ((protocol == null) ? 0 : protocol.GetHashCode());
				result = Prime * result + ((rpcKind == null) ? 0 : rpcKind.GetHashCode());
				return result;
			}

			public override bool Equals(object other)
			{
				//Object
				if (other == this)
				{
					return true;
				}
				if (other is RpcClientUtil.ProtoSigCacheKey)
				{
					RpcClientUtil.ProtoSigCacheKey otherKey = (RpcClientUtil.ProtoSigCacheKey)other;
					return (serverAddress.Equals(otherKey.serverAddress) && protocol.Equals(otherKey.
						protocol) && rpcKind.Equals(otherKey.rpcKind));
				}
				return false;
			}
		}

		private static ConcurrentHashMap<RpcClientUtil.ProtoSigCacheKey, IDictionary<long
			, ProtocolSignature>> signatureMap = new ConcurrentHashMap<RpcClientUtil.ProtoSigCacheKey
			, IDictionary<long, ProtocolSignature>>();

		private static void PutVersionSignatureMap(IPEndPoint addr, string protocol, string
			 rpcKind, IDictionary<long, ProtocolSignature> map)
		{
			signatureMap[new RpcClientUtil.ProtoSigCacheKey(addr, protocol, rpcKind)] = map;
		}

		private static IDictionary<long, ProtocolSignature> GetVersionSignatureMap(IPEndPoint
			 addr, string protocol, string rpcKind)
		{
			return signatureMap[new RpcClientUtil.ProtoSigCacheKey(addr, protocol, rpcKind)];
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
		public static bool IsMethodSupported(object rpcProxy, Type protocol, RPC.RpcKind 
			rpcKind, long version, string methodName)
		{
			IPEndPoint serverAddress = RPC.GetServerAddress(rpcProxy);
			IDictionary<long, ProtocolSignature> versionMap = GetVersionSignatureMap(serverAddress
				, protocol.FullName, rpcKind.ToString());
			if (versionMap == null)
			{
				Configuration conf = new Configuration();
				RPC.SetProtocolEngine(conf, typeof(ProtocolMetaInfoPB), typeof(ProtobufRpcEngine)
					);
				ProtocolMetaInfoPB protocolInfoProxy = GetProtocolMetaInfoProxy(rpcProxy, conf);
				ProtocolInfoProtos.GetProtocolSignatureRequestProto.Builder builder = ProtocolInfoProtos.GetProtocolSignatureRequestProto
					.NewBuilder();
				builder.SetProtocol(protocol.FullName);
				builder.SetRpcKind(rpcKind.ToString());
				ProtocolInfoProtos.GetProtocolSignatureResponseProto resp;
				try
				{
					resp = protocolInfoProxy.GetProtocolSignature(NullController, ((ProtocolInfoProtos.GetProtocolSignatureRequestProto
						)builder.Build()));
				}
				catch (ServiceException se)
				{
					throw ProtobufHelper.GetRemoteException(se);
				}
				versionMap = ConvertProtocolSignatureProtos(resp.GetProtocolSignatureList());
				PutVersionSignatureMap(serverAddress, protocol.FullName, rpcKind.ToString(), versionMap
					);
			}
			// Assuming unique method names.
			MethodInfo desiredMethod;
			MethodInfo[] allMethods = protocol.GetMethods();
			desiredMethod = null;
			foreach (MethodInfo m in allMethods)
			{
				if (m.Name.Equals(methodName))
				{
					desiredMethod = m;
					break;
				}
			}
			if (desiredMethod == null)
			{
				return false;
			}
			int methodHash = ProtocolSignature.GetFingerprint(desiredMethod);
			return MethodExists(methodHash, version, versionMap);
		}

		private static IDictionary<long, ProtocolSignature> ConvertProtocolSignatureProtos
			(IList<ProtocolInfoProtos.ProtocolSignatureProto> protoList)
		{
			IDictionary<long, ProtocolSignature> map = new SortedDictionary<long, ProtocolSignature
				>();
			foreach (ProtocolInfoProtos.ProtocolSignatureProto p in protoList)
			{
				int[] methods = new int[p.GetMethodsList().Count];
				int index = 0;
				foreach (int m in p.GetMethodsList())
				{
					methods[index++] = m;
				}
				map[p.GetVersion()] = new ProtocolSignature(p.GetVersion(), methods);
			}
			return map;
		}

		private static bool MethodExists(int methodHash, long version, IDictionary<long, 
			ProtocolSignature> versionMap)
		{
			ProtocolSignature sig = versionMap[version];
			if (sig != null)
			{
				foreach (int m in sig.GetMethods())
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
		private static ProtocolMetaInfoPB GetProtocolMetaInfoProxy(object proxy, Configuration
			 conf)
		{
			RpcInvocationHandler inv = (RpcInvocationHandler)Proxy.GetInvocationHandler(proxy
				);
			return RPC.GetProtocolEngine(typeof(ProtocolMetaInfoPB), conf).GetProtocolMetaInfoProxy
				(inv.GetConnectionId(), conf, NetUtils.GetDefaultSocketFactory(conf)).GetProxy();
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
		public static string MethodToTraceString(MethodInfo method)
		{
			Type clazz = method.DeclaringType;
			while (true)
			{
				Type next = clazz.GetEnclosingClass();
				if (next == null || next.GetEnclosingClass() == null)
				{
					break;
				}
				clazz = next;
			}
			return clazz.Name + "#" + method.Name;
		}
	}
}
