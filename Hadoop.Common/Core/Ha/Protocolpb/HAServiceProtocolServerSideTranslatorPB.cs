using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.Ipc;


namespace Org.Apache.Hadoop.HA.ProtocolPB
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
	public class HAServiceProtocolServerSideTranslatorPB : HAServiceProtocolPB
	{
		private readonly HAServiceProtocol server;

		private static readonly HAServiceProtocolProtos.MonitorHealthResponseProto MonitorHealthResp
			 = ((HAServiceProtocolProtos.MonitorHealthResponseProto)HAServiceProtocolProtos.MonitorHealthResponseProto
			.NewBuilder().Build());

		private static readonly HAServiceProtocolProtos.TransitionToActiveResponseProto TransitionToActiveResp
			 = ((HAServiceProtocolProtos.TransitionToActiveResponseProto)HAServiceProtocolProtos.TransitionToActiveResponseProto
			.NewBuilder().Build());

		private static readonly HAServiceProtocolProtos.TransitionToStandbyResponseProto 
			TransitionToStandbyResp = ((HAServiceProtocolProtos.TransitionToStandbyResponseProto
			)HAServiceProtocolProtos.TransitionToStandbyResponseProto.NewBuilder().Build());

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.ProtocolPB.HAServiceProtocolServerSideTranslatorPB
			));

		public HAServiceProtocolServerSideTranslatorPB(HAServiceProtocol server)
		{
			this.server = server;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HAServiceProtocolProtos.MonitorHealthResponseProto MonitorHealth(RpcController
			 controller, HAServiceProtocolProtos.MonitorHealthRequestProto request)
		{
			try
			{
				server.MonitorHealth();
				return MonitorHealthResp;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		private HAServiceProtocol.StateChangeRequestInfo Convert(HAServiceProtocolProtos.HAStateChangeRequestInfoProto
			 proto)
		{
			HAServiceProtocol.RequestSource src;
			switch (proto.GetReqSource())
			{
				case HAServiceProtocolProtos.HARequestSource.RequestByUser:
				{
					src = HAServiceProtocol.RequestSource.RequestByUser;
					break;
				}

				case HAServiceProtocolProtos.HARequestSource.RequestByUserForced:
				{
					src = HAServiceProtocol.RequestSource.RequestByUserForced;
					break;
				}

				case HAServiceProtocolProtos.HARequestSource.RequestByZkfc:
				{
					src = HAServiceProtocol.RequestSource.RequestByZkfc;
					break;
				}

				default:
				{
					Log.Warn("Unknown request source: " + proto.GetReqSource());
					src = null;
					break;
				}
			}
			return new HAServiceProtocol.StateChangeRequestInfo(src);
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HAServiceProtocolProtos.TransitionToActiveResponseProto TransitionToActive
			(RpcController controller, HAServiceProtocolProtos.TransitionToActiveRequestProto
			 request)
		{
			try
			{
				server.TransitionToActive(Convert(request.GetReqInfo()));
				return TransitionToActiveResp;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HAServiceProtocolProtos.TransitionToStandbyResponseProto TransitionToStandby
			(RpcController controller, HAServiceProtocolProtos.TransitionToStandbyRequestProto
			 request)
		{
			try
			{
				server.TransitionToStandby(Convert(request.GetReqInfo()));
				return TransitionToStandbyResp;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HAServiceProtocolProtos.GetServiceStatusResponseProto GetServiceStatus
			(RpcController controller, HAServiceProtocolProtos.GetServiceStatusRequestProto 
			request)
		{
			HAServiceStatus s;
			try
			{
				s = server.GetServiceStatus();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			HAServiceProtocolProtos.HAServiceStateProto retState;
			switch (s.GetState())
			{
				case HAServiceProtocol.HAServiceState.Active:
				{
					retState = HAServiceProtocolProtos.HAServiceStateProto.Active;
					break;
				}

				case HAServiceProtocol.HAServiceState.Standby:
				{
					retState = HAServiceProtocolProtos.HAServiceStateProto.Standby;
					break;
				}

				case HAServiceProtocol.HAServiceState.Initializing:
				default:
				{
					retState = HAServiceProtocolProtos.HAServiceStateProto.Initializing;
					break;
				}
			}
			HAServiceProtocolProtos.GetServiceStatusResponseProto.Builder ret = HAServiceProtocolProtos.GetServiceStatusResponseProto
				.NewBuilder().SetState(retState).SetReadyToBecomeActive(s.IsReadyToBecomeActive(
				));
			if (!s.IsReadyToBecomeActive())
			{
				ret.SetNotReadyReason(s.GetNotReadyReason());
			}
			return ((HAServiceProtocolProtos.GetServiceStatusResponseProto)ret.Build());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string protocol, long clientVersion)
		{
			return RPC.GetProtocolVersion(typeof(HAServiceProtocolPB));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
			, int clientMethodsHash)
		{
			if (!protocol.Equals(RPC.GetProtocolName(typeof(HAServiceProtocolPB))))
			{
				throw new IOException("Serverside implements " + RPC.GetProtocolName(typeof(HAServiceProtocolPB
					)) + ". The following requested protocol is unknown: " + protocol);
			}
			return ProtocolSignature.GetProtocolSignature(clientMethodsHash, RPC.GetProtocolVersion
				(typeof(HAServiceProtocolPB)), typeof(HAServiceProtocolPB));
		}
	}
}
