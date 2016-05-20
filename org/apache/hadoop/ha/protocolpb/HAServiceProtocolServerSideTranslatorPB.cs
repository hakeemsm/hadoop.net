using Sharpen;

namespace org.apache.hadoop.ha.protocolPB
{
	/// <summary>This class is used on the server side.</summary>
	/// <remarks>
	/// This class is used on the server side. Calls come across the wire for the
	/// for protocol
	/// <see cref="HAServiceProtocolPB"/>
	/// .
	/// This class translates the PB data types
	/// to the native data types used inside the NN as specified in the generic
	/// ClientProtocol.
	/// </remarks>
	public class HAServiceProtocolServerSideTranslatorPB : org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB
	{
		private readonly org.apache.hadoop.ha.HAServiceProtocol server;

		private static readonly org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthResponseProto
			 MONITOR_HEALTH_RESP = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthResponseProto
			)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthResponseProto.newBuilder
			().build());

		private static readonly org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveResponseProto
			 TRANSITION_TO_ACTIVE_RESP = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveResponseProto
			)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveResponseProto
			.newBuilder().build());

		private static readonly org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyResponseProto
			 TRANSITION_TO_STANDBY_RESP = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyResponseProto
			)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyResponseProto
			.newBuilder().build());

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB
			)));

		public HAServiceProtocolServerSideTranslatorPB(org.apache.hadoop.ha.HAServiceProtocol
			 server)
		{
			this.server = server;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthResponseProto
			 monitorHealth(com.google.protobuf.RpcController controller, org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto
			 request)
		{
			try
			{
				server.monitorHealth();
				return MONITOR_HEALTH_RESP;
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		private org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo convert(org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAStateChangeRequestInfoProto
			 proto)
		{
			org.apache.hadoop.ha.HAServiceProtocol.RequestSource src;
			switch (proto.getReqSource())
			{
				case org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource.REQUEST_BY_USER
					:
				{
					src = org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER;
					break;
				}

				case org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource.REQUEST_BY_USER_FORCED
					:
				{
					src = org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED;
					break;
				}

				case org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource.REQUEST_BY_ZKFC
					:
				{
					src = org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC;
					break;
				}

				default:
				{
					LOG.warn("Unknown request source: " + proto.getReqSource());
					src = null;
					break;
				}
			}
			return new org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo(src);
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveResponseProto
			 transitionToActive(com.google.protobuf.RpcController controller, org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto
			 request)
		{
			try
			{
				server.transitionToActive(convert(request.getReqInfo()));
				return TRANSITION_TO_ACTIVE_RESP;
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyResponseProto
			 transitionToStandby(com.google.protobuf.RpcController controller, org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto
			 request)
		{
			try
			{
				server.transitionToStandby(convert(request.getReqInfo()));
				return TRANSITION_TO_STANDBY_RESP;
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto
			 getServiceStatus(com.google.protobuf.RpcController controller, org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto
			 request)
		{
			org.apache.hadoop.ha.HAServiceStatus s;
			try
			{
				s = server.getServiceStatus();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
			org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto retState;
			switch (s.getState())
			{
				case org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE:
				{
					retState = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto
						.ACTIVE;
					break;
				}

				case org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY:
				{
					retState = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto
						.STANDBY;
					break;
				}

				case org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING:
				default:
				{
					retState = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto
						.INITIALIZING;
					break;
				}
			}
			org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto.Builder
				 ret = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto
				.newBuilder().setState(retState).setReadyToBecomeActive(s.isReadyToBecomeActive(
				));
			if (!s.isReadyToBecomeActive())
			{
				ret.setNotReadyReason(s.getNotReadyReason());
			}
			return ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto
				)ret.build());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long getProtocolVersion(string protocol, long clientVersion)
		{
			return org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
			 protocol, long clientVersion, int clientMethodsHash)
		{
			if (!protocol.Equals(org.apache.hadoop.ipc.RPC.getProtocolName(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)))))
			{
				throw new System.IO.IOException("Serverside implements " + org.apache.hadoop.ipc.RPC
					.getProtocolName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB
					))) + ". The following requested protocol is unknown: " + protocol);
			}
			return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
				, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB))), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)));
		}
	}
}
