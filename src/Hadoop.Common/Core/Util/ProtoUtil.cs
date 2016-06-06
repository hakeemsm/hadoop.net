using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Htrace;


namespace Org.Apache.Hadoop.Util
{
	public abstract class ProtoUtil
	{
		/// <summary>Read a variable length integer in the same format that ProtoBufs encodes.
		/// 	</summary>
		/// <param name="in">the input stream to read from</param>
		/// <returns>the integer</returns>
		/// <exception cref="System.IO.IOException">if it is malformed or EOF.</exception>
		public static int ReadRawVarint32(BinaryReader reader)
		{
			byte tmp = @in.ReadByte();
			if (tmp >= 0)
			{
				return tmp;
			}
			int result = tmp & unchecked((int)(0x7f));
			if ((tmp = @in.ReadByte()) >= 0)
			{
				result |= tmp << 7;
			}
			else
			{
				result |= (tmp & unchecked((int)(0x7f))) << 7;
				if ((tmp = @in.ReadByte()) >= 0)
				{
					result |= tmp << 14;
				}
				else
				{
					result |= (tmp & unchecked((int)(0x7f))) << 14;
					if ((tmp = @in.ReadByte()) >= 0)
					{
						result |= tmp << 21;
					}
					else
					{
						result |= (tmp & unchecked((int)(0x7f))) << 21;
						result |= (tmp = @in.ReadByte()) << 28;
						if (((sbyte)tmp) < 0)
						{
							// Discard upper 32 bits.
							for (int i = 0; i < 5; i++)
							{
								if (@in.ReadByte() >= 0)
								{
									return result;
								}
							}
							throw new IOException("Malformed varint");
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
		public static IpcConnectionContextProtos.IpcConnectionContextProto MakeIpcConnectionContext
			(string protocol, UserGroupInformation ugi, SaslRpcServer.AuthMethod authMethod)
		{
			IpcConnectionContextProtos.IpcConnectionContextProto.Builder result = IpcConnectionContextProtos.IpcConnectionContextProto
				.NewBuilder();
			if (protocol != null)
			{
				result.SetProtocol(protocol);
			}
			IpcConnectionContextProtos.UserInformationProto.Builder ugiProto = IpcConnectionContextProtos.UserInformationProto
				.NewBuilder();
			if (ugi != null)
			{
				/*
				* In the connection context we send only additional user info that
				* is not derived from the authentication done during connection setup.
				*/
				if (authMethod == SaslRpcServer.AuthMethod.Kerberos)
				{
					// Real user was established as part of the connection.
					// Send effective user only.
					ugiProto.SetEffectiveUser(ugi.GetUserName());
				}
				else
				{
					if (authMethod == SaslRpcServer.AuthMethod.Token)
					{
					}
					else
					{
						// With token, the connection itself establishes 
						// both real and effective user. Hence send none in header.
						// Simple authentication
						// No user info is established as part of the connection.
						// Send both effective user and real user
						ugiProto.SetEffectiveUser(ugi.GetUserName());
						if (ugi.GetRealUser() != null)
						{
							ugiProto.SetRealUser(ugi.GetRealUser().GetUserName());
						}
					}
				}
			}
			result.SetUserInfo(ugiProto);
			return ((IpcConnectionContextProtos.IpcConnectionContextProto)result.Build());
		}

		public static UserGroupInformation GetUgi(IpcConnectionContextProtos.IpcConnectionContextProto
			 context)
		{
			if (context.HasUserInfo())
			{
				IpcConnectionContextProtos.UserInformationProto userInfo = context.GetUserInfo();
				return GetUgi(userInfo);
			}
			else
			{
				return null;
			}
		}

		public static UserGroupInformation GetUgi(IpcConnectionContextProtos.UserInformationProto
			 userInfo)
		{
			UserGroupInformation ugi = null;
			string effectiveUser = userInfo.HasEffectiveUser() ? userInfo.GetEffectiveUser() : 
				null;
			string realUser = userInfo.HasRealUser() ? userInfo.GetRealUser() : null;
			if (effectiveUser != null)
			{
				if (realUser != null)
				{
					UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(realUser
						);
					ugi = UserGroupInformation.CreateProxyUser(effectiveUser, realUserUgi);
				}
				else
				{
					ugi = UserGroupInformation.CreateRemoteUser(effectiveUser);
				}
			}
			return ugi;
		}

		internal static RpcHeaderProtos.RpcKindProto Convert(RPC.RpcKind kind)
		{
			switch (kind)
			{
				case RPC.RpcKind.RpcBuiltin:
				{
					return RpcHeaderProtos.RpcKindProto.RpcBuiltin;
				}

				case RPC.RpcKind.RpcWritable:
				{
					return RpcHeaderProtos.RpcKindProto.RpcWritable;
				}

				case RPC.RpcKind.RpcProtocolBuffer:
				{
					return RpcHeaderProtos.RpcKindProto.RpcProtocolBuffer;
				}
			}
			return null;
		}

		public static RPC.RpcKind Convert(RpcHeaderProtos.RpcKindProto kind)
		{
			switch (kind)
			{
				case RpcHeaderProtos.RpcKindProto.RpcBuiltin:
				{
					return RPC.RpcKind.RpcBuiltin;
				}

				case RpcHeaderProtos.RpcKindProto.RpcWritable:
				{
					return RPC.RpcKind.RpcWritable;
				}

				case RpcHeaderProtos.RpcKindProto.RpcProtocolBuffer:
				{
					return RPC.RpcKind.RpcProtocolBuffer;
				}
			}
			return null;
		}

		public static RpcHeaderProtos.RpcRequestHeaderProto MakeRpcRequestHeader(RPC.RpcKind
			 rpcKind, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto operation, int callId
			, int retryCount, byte[] uuid)
		{
			RpcHeaderProtos.RpcRequestHeaderProto.Builder result = RpcHeaderProtos.RpcRequestHeaderProto
				.NewBuilder();
			result.SetRpcKind(Convert(rpcKind)).SetRpcOp(operation).SetCallId(callId).SetRetryCount
				(retryCount).SetClientId(ByteString.CopyFrom(uuid));
			// Add tracing info if we are currently tracing.
			if (Trace.IsTracing())
			{
				Span s = Trace.CurrentSpan();
				result.SetTraceInfo(((RpcHeaderProtos.RPCTraceInfoProto)RpcHeaderProtos.RPCTraceInfoProto
					.NewBuilder().SetParentId(s.GetSpanId()).SetTraceId(s.GetTraceId()).Build()));
			}
			return ((RpcHeaderProtos.RpcRequestHeaderProto)result.Build());
		}
	}
}
