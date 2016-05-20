using Sharpen;

namespace org.apache.hadoop.util
{
	public abstract class ProtoUtil
	{
		/// <summary>Read a variable length integer in the same format that ProtoBufs encodes.
		/// 	</summary>
		/// <param name="in">the input stream to read from</param>
		/// <returns>the integer</returns>
		/// <exception cref="System.IO.IOException">if it is malformed or EOF.</exception>
		public static int readRawVarint32(java.io.DataInput @in)
		{
			byte tmp = @in.readByte();
			if (tmp >= 0)
			{
				return tmp;
			}
			int result = tmp & unchecked((int)(0x7f));
			if ((tmp = @in.readByte()) >= 0)
			{
				result |= tmp << 7;
			}
			else
			{
				result |= (tmp & unchecked((int)(0x7f))) << 7;
				if ((tmp = @in.readByte()) >= 0)
				{
					result |= tmp << 14;
				}
				else
				{
					result |= (tmp & unchecked((int)(0x7f))) << 14;
					if ((tmp = @in.readByte()) >= 0)
					{
						result |= tmp << 21;
					}
					else
					{
						result |= (tmp & unchecked((int)(0x7f))) << 21;
						result |= (tmp = @in.readByte()) << 28;
						if (((sbyte)tmp) < 0)
						{
							// Discard upper 32 bits.
							for (int i = 0; i < 5; i++)
							{
								if (@in.readByte() >= 0)
								{
									return result;
								}
							}
							throw new System.IO.IOException("Malformed varint");
						}
					}
				}
			}
			return result;
		}

		/// <summary>
		/// This method creates the connection context  using exactly the same logic
		/// as the old connection context as was done for writable where
		/// the effective and real users are set based on the auth method.
		/// </summary>
		public static org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
			 makeIpcConnectionContext(string protocol, org.apache.hadoop.security.UserGroupInformation
			 ugi, org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod)
		{
			org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto.Builder
				 result = org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
				.newBuilder();
			if (protocol != null)
			{
				result.setProtocol(protocol);
			}
			org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto.Builder
				 ugiProto = org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto
				.newBuilder();
			if (ugi != null)
			{
				/*
				* In the connection context we send only additional user info that
				* is not derived from the authentication done during connection setup.
				*/
				if (authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS)
				{
					// Real user was established as part of the connection.
					// Send effective user only.
					ugiProto.setEffectiveUser(ugi.getUserName());
				}
				else
				{
					if (authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN)
					{
					}
					else
					{
						// With token, the connection itself establishes 
						// both real and effective user. Hence send none in header.
						// Simple authentication
						// No user info is established as part of the connection.
						// Send both effective user and real user
						ugiProto.setEffectiveUser(ugi.getUserName());
						if (ugi.getRealUser() != null)
						{
							ugiProto.setRealUser(ugi.getRealUser().getUserName());
						}
					}
				}
			}
			result.setUserInfo(ugiProto);
			return ((org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
				)result.build());
		}

		public static org.apache.hadoop.security.UserGroupInformation getUgi(org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
			 context)
		{
			if (context.hasUserInfo())
			{
				org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto userInfo
					 = context.getUserInfo();
				return getUgi(userInfo);
			}
			else
			{
				return null;
			}
		}

		public static org.apache.hadoop.security.UserGroupInformation getUgi(org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto
			 userInfo)
		{
			org.apache.hadoop.security.UserGroupInformation ugi = null;
			string effectiveUser = userInfo.hasEffectiveUser() ? userInfo.getEffectiveUser() : 
				null;
			string realUser = userInfo.hasRealUser() ? userInfo.getRealUser() : null;
			if (effectiveUser != null)
			{
				if (realUser != null)
				{
					org.apache.hadoop.security.UserGroupInformation realUserUgi = org.apache.hadoop.security.UserGroupInformation
						.createRemoteUser(realUser);
					ugi = org.apache.hadoop.security.UserGroupInformation.createProxyUser(effectiveUser
						, realUserUgi);
				}
				else
				{
					ugi = org.apache.hadoop.security.UserGroupInformation.createRemoteUser(effectiveUser
						);
				}
			}
			return ugi;
		}

		internal static org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto convert
			(org.apache.hadoop.ipc.RPC.RpcKind kind)
		{
			switch (kind)
			{
				case org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN:
				{
					return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto.RPC_BUILTIN;
				}

				case org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE:
				{
					return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto.RPC_WRITABLE;
				}

				case org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER:
				{
					return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto.RPC_PROTOCOL_BUFFER;
				}
			}
			return null;
		}

		public static org.apache.hadoop.ipc.RPC.RpcKind convert(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto
			 kind)
		{
			switch (kind)
			{
				case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto.RPC_BUILTIN:
				{
					return org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN;
				}

				case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto.RPC_WRITABLE:
				{
					return org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE;
				}

				case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto.RPC_PROTOCOL_BUFFER
					:
				{
					return org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER;
				}
			}
			return null;
		}

		public static org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
			 makeRpcRequestHeader(org.apache.hadoop.ipc.RPC.RpcKind rpcKind, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
			 operation, int callId, int retryCount, byte[] uuid)
		{
			org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.Builder result
				 = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.newBuilder
				();
			result.setRpcKind(convert(rpcKind)).setRpcOp(operation).setCallId(callId).setRetryCount
				(retryCount).setClientId(com.google.protobuf.ByteString.copyFrom(uuid));
			// Add tracing info if we are currently tracing.
			if (org.apache.htrace.Trace.isTracing())
			{
				org.apache.htrace.Span s = org.apache.htrace.Trace.currentSpan();
				result.setTraceInfo(((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RPCTraceInfoProto
					)org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RPCTraceInfoProto.newBuilder().setParentId
					(s.getSpanId()).setTraceId(s.getTraceId()).build()));
			}
			return ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto)result
				.build());
		}
	}
}
