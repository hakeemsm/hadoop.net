using System;
using System.Net;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.HA.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.HA.HAServiceProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="HAServiceProtocolPB"/>
	/// .
	/// </summary>
	public class HAServiceProtocolClientSideTranslatorPB : HAServiceProtocol, IDisposable
		, ProtocolTranslator
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private static readonly HAServiceProtocolProtos.MonitorHealthRequestProto MonitorHealthReq
			 = ((HAServiceProtocolProtos.MonitorHealthRequestProto)HAServiceProtocolProtos.MonitorHealthRequestProto
			.NewBuilder().Build());

		private static readonly HAServiceProtocolProtos.GetServiceStatusRequestProto GetServiceStatusReq
			 = ((HAServiceProtocolProtos.GetServiceStatusRequestProto)HAServiceProtocolProtos.GetServiceStatusRequestProto
			.NewBuilder().Build());

		private readonly HAServiceProtocolPB rpcProxy;

		/// <exception cref="System.IO.IOException"/>
		public HAServiceProtocolClientSideTranslatorPB(IPEndPoint addr, Configuration conf
			)
		{
			RPC.SetProtocolEngine(conf, typeof(HAServiceProtocolPB), typeof(ProtobufRpcEngine
				));
			rpcProxy = RPC.GetProxy<HAServiceProtocolPB>(RPC.GetProtocolVersion(typeof(HAServiceProtocolPB
				)), addr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public HAServiceProtocolClientSideTranslatorPB(IPEndPoint addr, Configuration conf
			, SocketFactory socketFactory, int timeout)
		{
			RPC.SetProtocolEngine(conf, typeof(HAServiceProtocolPB), typeof(ProtobufRpcEngine
				));
			rpcProxy = RPC.GetProxy<HAServiceProtocolPB>(RPC.GetProtocolVersion(typeof(HAServiceProtocolPB
				)), addr, UserGroupInformation.GetCurrentUser(), conf, socketFactory, timeout);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void MonitorHealth()
		{
			try
			{
				rpcProxy.MonitorHealth(NullController, MonitorHealthReq);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TransitionToActive(HAServiceProtocol.StateChangeRequestInfo reqInfo
			)
		{
			try
			{
				HAServiceProtocolProtos.TransitionToActiveRequestProto req = ((HAServiceProtocolProtos.TransitionToActiveRequestProto
					)HAServiceProtocolProtos.TransitionToActiveRequestProto.NewBuilder().SetReqInfo(
					Convert(reqInfo)).Build());
				rpcProxy.TransitionToActive(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TransitionToStandby(HAServiceProtocol.StateChangeRequestInfo 
			reqInfo)
		{
			try
			{
				HAServiceProtocolProtos.TransitionToStandbyRequestProto req = ((HAServiceProtocolProtos.TransitionToStandbyRequestProto
					)HAServiceProtocolProtos.TransitionToStandbyRequestProto.NewBuilder().SetReqInfo
					(Convert(reqInfo)).Build());
				rpcProxy.TransitionToStandby(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HAServiceStatus GetServiceStatus()
		{
			HAServiceProtocolProtos.GetServiceStatusResponseProto status;
			try
			{
				status = rpcProxy.GetServiceStatus(NullController, GetServiceStatusReq);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			HAServiceStatus ret = new HAServiceStatus(Convert(status.GetState()));
			if (status.GetReadyToBecomeActive())
			{
				ret.SetReadyToBecomeActive();
			}
			else
			{
				ret.SetNotReadyToBecomeActive(status.GetNotReadyReason());
			}
			return ret;
		}

		private HAServiceProtocol.HAServiceState Convert(HAServiceProtocolProtos.HAServiceStateProto
			 state)
		{
			switch (state)
			{
				case HAServiceProtocolProtos.HAServiceStateProto.Active:
				{
					return HAServiceProtocol.HAServiceState.Active;
				}

				case HAServiceProtocolProtos.HAServiceStateProto.Standby:
				{
					return HAServiceProtocol.HAServiceState.Standby;
				}

				case HAServiceProtocolProtos.HAServiceStateProto.Initializing:
				default:
				{
					return HAServiceProtocol.HAServiceState.Initializing;
				}
			}
		}

		private HAServiceProtocolProtos.HAStateChangeRequestInfoProto Convert(HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			HAServiceProtocolProtos.HARequestSource src;
			switch (reqInfo.GetSource())
			{
				case HAServiceProtocol.RequestSource.RequestByUser:
				{
					src = HAServiceProtocolProtos.HARequestSource.RequestByUser;
					break;
				}

				case HAServiceProtocol.RequestSource.RequestByUserForced:
				{
					src = HAServiceProtocolProtos.HARequestSource.RequestByUserForced;
					break;
				}

				case HAServiceProtocol.RequestSource.RequestByZkfc:
				{
					src = HAServiceProtocolProtos.HARequestSource.RequestByZkfc;
					break;
				}

				default:
				{
					throw new ArgumentException("Bad source: " + reqInfo.GetSource());
				}
			}
			return ((HAServiceProtocolProtos.HAStateChangeRequestInfoProto)HAServiceProtocolProtos.HAStateChangeRequestInfoProto
				.NewBuilder().SetReqSource(src).Build());
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		public virtual object GetUnderlyingProxyObject()
		{
			return rpcProxy;
		}
	}
}
