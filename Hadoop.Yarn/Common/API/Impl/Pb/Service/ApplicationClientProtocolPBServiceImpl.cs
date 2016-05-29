using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Impl.PB.Service
{
	public class ApplicationClientProtocolPBServiceImpl : ApplicationClientProtocolPB
	{
		private ApplicationClientProtocol real;

		public ApplicationClientProtocolPBServiceImpl(ApplicationClientProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.KillApplicationResponseProto ForceKillApplication
			(RpcController arg0, YarnServiceProtos.KillApplicationRequestProto proto)
		{
			KillApplicationRequestPBImpl request = new KillApplicationRequestPBImpl(proto);
			try
			{
				KillApplicationResponse response = real.ForceKillApplication(request);
				return ((KillApplicationResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetApplicationReportResponseProto GetApplicationReport
			(RpcController arg0, YarnServiceProtos.GetApplicationReportRequestProto proto)
		{
			GetApplicationReportRequestPBImpl request = new GetApplicationReportRequestPBImpl
				(proto);
			try
			{
				GetApplicationReportResponse response = real.GetApplicationReport(request);
				return ((GetApplicationReportResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetClusterMetricsResponseProto GetClusterMetrics
			(RpcController arg0, YarnServiceProtos.GetClusterMetricsRequestProto proto)
		{
			GetClusterMetricsRequestPBImpl request = new GetClusterMetricsRequestPBImpl(proto
				);
			try
			{
				GetClusterMetricsResponse response = real.GetClusterMetrics(request);
				return ((GetClusterMetricsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetNewApplicationResponseProto GetNewApplication
			(RpcController arg0, YarnServiceProtos.GetNewApplicationRequestProto proto)
		{
			GetNewApplicationRequestPBImpl request = new GetNewApplicationRequestPBImpl(proto
				);
			try
			{
				GetNewApplicationResponse response = real.GetNewApplication(request);
				return ((GetNewApplicationResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.SubmitApplicationResponseProto SubmitApplication
			(RpcController arg0, YarnServiceProtos.SubmitApplicationRequestProto proto)
		{
			SubmitApplicationRequestPBImpl request = new SubmitApplicationRequestPBImpl(proto
				);
			try
			{
				SubmitApplicationResponse response = real.SubmitApplication(request);
				return ((SubmitApplicationResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetApplicationsResponseProto GetApplications(RpcController
			 controller, YarnServiceProtos.GetApplicationsRequestProto proto)
		{
			GetApplicationsRequestPBImpl request = new GetApplicationsRequestPBImpl(proto);
			try
			{
				GetApplicationsResponse response = real.GetApplications(request);
				return ((GetApplicationsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetClusterNodesResponseProto GetClusterNodes(RpcController
			 controller, YarnServiceProtos.GetClusterNodesRequestProto proto)
		{
			GetClusterNodesRequestPBImpl request = new GetClusterNodesRequestPBImpl(proto);
			try
			{
				GetClusterNodesResponse response = real.GetClusterNodes(request);
				return ((GetClusterNodesResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetQueueInfoResponseProto GetQueueInfo(RpcController
			 controller, YarnServiceProtos.GetQueueInfoRequestProto proto)
		{
			GetQueueInfoRequestPBImpl request = new GetQueueInfoRequestPBImpl(proto);
			try
			{
				GetQueueInfoResponse response = real.GetQueueInfo(request);
				return ((GetQueueInfoResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetQueueUserAclsInfoResponseProto GetQueueUserAcls
			(RpcController controller, YarnServiceProtos.GetQueueUserAclsInfoRequestProto proto
			)
		{
			GetQueueUserAclsInfoRequestPBImpl request = new GetQueueUserAclsInfoRequestPBImpl
				(proto);
			try
			{
				GetQueueUserAclsInfoResponse response = real.GetQueueUserAcls(request);
				return ((GetQueueUserAclsInfoResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual SecurityProtos.GetDelegationTokenResponseProto GetDelegationToken(
			RpcController controller, SecurityProtos.GetDelegationTokenRequestProto proto)
		{
			GetDelegationTokenRequestPBImpl request = new GetDelegationTokenRequestPBImpl(proto
				);
			try
			{
				GetDelegationTokenResponse response = real.GetDelegationToken(request);
				return ((GetDelegationTokenResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual SecurityProtos.RenewDelegationTokenResponseProto RenewDelegationToken
			(RpcController controller, SecurityProtos.RenewDelegationTokenRequestProto proto
			)
		{
			RenewDelegationTokenRequestPBImpl request = new RenewDelegationTokenRequestPBImpl
				(proto);
			try
			{
				RenewDelegationTokenResponse response = real.RenewDelegationToken(request);
				return ((RenewDelegationTokenResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual SecurityProtos.CancelDelegationTokenResponseProto CancelDelegationToken
			(RpcController controller, SecurityProtos.CancelDelegationTokenRequestProto proto
			)
		{
			CancelDelegationTokenRequestPBImpl request = new CancelDelegationTokenRequestPBImpl
				(proto);
			try
			{
				CancelDelegationTokenResponse response = real.CancelDelegationToken(request);
				return ((CancelDelegationTokenResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto MoveApplicationAcrossQueues
			(RpcController controller, YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto
			 proto)
		{
			MoveApplicationAcrossQueuesRequestPBImpl request = new MoveApplicationAcrossQueuesRequestPBImpl
				(proto);
			try
			{
				MoveApplicationAcrossQueuesResponse response = real.MoveApplicationAcrossQueues(request
					);
				return ((MoveApplicationAcrossQueuesResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetApplicationAttemptReportResponseProto GetApplicationAttemptReport
			(RpcController controller, YarnServiceProtos.GetApplicationAttemptReportRequestProto
			 proto)
		{
			GetApplicationAttemptReportRequestPBImpl request = new GetApplicationAttemptReportRequestPBImpl
				(proto);
			try
			{
				GetApplicationAttemptReportResponse response = real.GetApplicationAttemptReport(request
					);
				return ((GetApplicationAttemptReportResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetApplicationAttemptsResponseProto GetApplicationAttempts
			(RpcController controller, YarnServiceProtos.GetApplicationAttemptsRequestProto 
			proto)
		{
			GetApplicationAttemptsRequestPBImpl request = new GetApplicationAttemptsRequestPBImpl
				(proto);
			try
			{
				GetApplicationAttemptsResponse response = real.GetApplicationAttempts(request);
				return ((GetApplicationAttemptsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetContainerReportResponseProto GetContainerReport
			(RpcController controller, YarnServiceProtos.GetContainerReportRequestProto proto
			)
		{
			GetContainerReportRequestPBImpl request = new GetContainerReportRequestPBImpl(proto
				);
			try
			{
				GetContainerReportResponse response = real.GetContainerReport(request);
				return ((GetContainerReportResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetContainersResponseProto GetContainers(RpcController
			 controller, YarnServiceProtos.GetContainersRequestProto proto)
		{
			GetContainersRequestPBImpl request = new GetContainersRequestPBImpl(proto);
			try
			{
				GetContainersResponse response = real.GetContainers(request);
				return ((GetContainersResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.ReservationSubmissionResponseProto SubmitReservation
			(RpcController controller, YarnServiceProtos.ReservationSubmissionRequestProto requestProto
			)
		{
			ReservationSubmissionRequestPBImpl request = new ReservationSubmissionRequestPBImpl
				(requestProto);
			try
			{
				ReservationSubmissionResponse response = real.SubmitReservation(request);
				return ((ReservationSubmissionResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.ReservationUpdateResponseProto UpdateReservation
			(RpcController controller, YarnServiceProtos.ReservationUpdateRequestProto requestProto
			)
		{
			ReservationUpdateRequestPBImpl request = new ReservationUpdateRequestPBImpl(requestProto
				);
			try
			{
				ReservationUpdateResponse response = real.UpdateReservation(request);
				return ((ReservationUpdateResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.ReservationDeleteResponseProto DeleteReservation
			(RpcController controller, YarnServiceProtos.ReservationDeleteRequestProto requestProto
			)
		{
			ReservationDeleteRequestPBImpl request = new ReservationDeleteRequestPBImpl(requestProto
				);
			try
			{
				ReservationDeleteResponse response = real.DeleteReservation(request);
				return ((ReservationDeleteResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetNodesToLabelsResponseProto GetNodeToLabels(RpcController
			 controller, YarnServiceProtos.GetNodesToLabelsRequestProto proto)
		{
			GetNodesToLabelsRequestPBImpl request = new GetNodesToLabelsRequestPBImpl(proto);
			try
			{
				GetNodesToLabelsResponse response = real.GetNodeToLabels(request);
				return ((GetNodesToLabelsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetLabelsToNodesResponseProto GetLabelsToNodes(RpcController
			 controller, YarnServiceProtos.GetLabelsToNodesRequestProto proto)
		{
			GetLabelsToNodesRequestPBImpl request = new GetLabelsToNodesRequestPBImpl(proto);
			try
			{
				GetLabelsToNodesResponse response = real.GetLabelsToNodes(request);
				return ((GetLabelsToNodesResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.GetClusterNodeLabelsResponseProto GetClusterNodeLabels
			(RpcController controller, YarnServiceProtos.GetClusterNodeLabelsRequestProto proto
			)
		{
			GetClusterNodeLabelsRequestPBImpl request = new GetClusterNodeLabelsRequestPBImpl
				(proto);
			try
			{
				GetClusterNodeLabelsResponse response = real.GetClusterNodeLabels(request);
				return ((GetClusterNodeLabelsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
