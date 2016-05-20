using Sharpen;

namespace org.apache.hadoop.ha.protocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="org.apache.hadoop.ha.HAServiceProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="HAServiceProtocolPB"/>
	/// .
	/// </summary>
	public class HAServiceProtocolClientSideTranslatorPB : org.apache.hadoop.ha.HAServiceProtocol
		, java.io.Closeable, org.apache.hadoop.ipc.ProtocolTranslator
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private static readonly org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto
			 MONITOR_HEALTH_REQ = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto
			)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto.newBuilder
			().build());

		private static readonly org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto
			 GET_SERVICE_STATUS_REQ = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto
			)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto
			.newBuilder().build());

		private readonly org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB rpcProxy;

		/// <exception cref="System.IO.IOException"/>
		public HAServiceProtocolClientSideTranslatorPB(java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 conf)
		{
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			rpcProxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB
				>(org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB))), addr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public HAServiceProtocolClientSideTranslatorPB(java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory socketFactory, int timeout)
		{
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			rpcProxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB
				>(org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB))), addr, org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser(), conf, socketFactory, timeout);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void monitorHealth()
		{
			try
			{
				rpcProxy.monitorHealth(NULL_CONTROLLER, MONITOR_HEALTH_REQ);
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void transitionToActive(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			try
			{
				org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto
					 req = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto
					)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto
					.newBuilder().setReqInfo(convert(reqInfo)).build());
				rpcProxy.transitionToActive(NULL_CONTROLLER, req);
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void transitionToStandby(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			try
			{
				org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto
					 req = ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto
					)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto
					.newBuilder().setReqInfo(convert(reqInfo)).build());
				rpcProxy.transitionToStandby(NULL_CONTROLLER, req);
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ha.HAServiceStatus getServiceStatus()
		{
			org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto 
				status;
			try
			{
				status = rpcProxy.getServiceStatus(NULL_CONTROLLER, GET_SERVICE_STATUS_REQ);
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
			org.apache.hadoop.ha.HAServiceStatus ret = new org.apache.hadoop.ha.HAServiceStatus
				(convert(status.getState()));
			if (status.getReadyToBecomeActive())
			{
				ret.setReadyToBecomeActive();
			}
			else
			{
				ret.setNotReadyToBecomeActive(status.getNotReadyReason());
			}
			return ret;
		}

		private org.apache.hadoop.ha.HAServiceProtocol.HAServiceState convert(org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto
			 state)
		{
			switch (state)
			{
				case org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto.ACTIVE
					:
				{
					return org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
				}

				case org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto.STANDBY
					:
				{
					return org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
				}

				case org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto.INITIALIZING
					:
				default:
				{
					return org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING;
				}
			}
		}

		private org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAStateChangeRequestInfoProto
			 convert(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo reqInfo)
		{
			org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource src;
			switch (reqInfo.getSource())
			{
				case org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER:
				{
					src = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource.REQUEST_BY_USER;
					break;
				}

				case org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED:
				{
					src = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource.REQUEST_BY_USER_FORCED;
					break;
				}

				case org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC:
				{
					src = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HARequestSource.REQUEST_BY_ZKFC;
					break;
				}

				default:
				{
					throw new System.ArgumentException("Bad source: " + reqInfo.getSource());
				}
			}
			return ((org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAStateChangeRequestInfoProto
				)org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAStateChangeRequestInfoProto
				.newBuilder().setReqSource(src).build());
		}

		public virtual void close()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(rpcProxy);
		}

		public virtual object getUnderlyingProxyObject()
		{
			return rpcProxy;
		}
	}
}
