using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Impl.PB.Client
{
	public class ApplicationClientProtocolPBClientImpl : ApplicationClientProtocol, IDisposable
	{
		private ApplicationClientProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ApplicationClientProtocolPBClientImpl(long clientVersion, IPEndPoint addr, 
			Configuration conf)
		{
			RPC.SetProtocolEngine(conf, typeof(ApplicationClientProtocolPB), typeof(ProtobufRpcEngine
				));
			proxy = RPC.GetProxy<ApplicationClientProtocolPB>(clientVersion, addr, conf);
		}

		public virtual void Close()
		{
			if (this.proxy != null)
			{
				RPC.StopProxy(this.proxy);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual KillApplicationResponse ForceKillApplication(KillApplicationRequest
			 request)
		{
			YarnServiceProtos.KillApplicationRequestProto requestProto = ((KillApplicationRequestPBImpl
				)request).GetProto();
			try
			{
				return new KillApplicationResponsePBImpl(proxy.ForceKillApplication(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationReportResponse GetApplicationReport(GetApplicationReportRequest
			 request)
		{
			YarnServiceProtos.GetApplicationReportRequestProto requestProto = ((GetApplicationReportRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetApplicationReportResponsePBImpl(proxy.GetApplicationReport(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetClusterMetricsResponse GetClusterMetrics(GetClusterMetricsRequest
			 request)
		{
			YarnServiceProtos.GetClusterMetricsRequestProto requestProto = ((GetClusterMetricsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetClusterMetricsResponsePBImpl(proxy.GetClusterMetrics(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetNewApplicationResponse GetNewApplication(GetNewApplicationRequest
			 request)
		{
			YarnServiceProtos.GetNewApplicationRequestProto requestProto = ((GetNewApplicationRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetNewApplicationResponsePBImpl(proxy.GetNewApplication(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual SubmitApplicationResponse SubmitApplication(SubmitApplicationRequest
			 request)
		{
			YarnServiceProtos.SubmitApplicationRequestProto requestProto = ((SubmitApplicationRequestPBImpl
				)request).GetProto();
			try
			{
				return new SubmitApplicationResponsePBImpl(proxy.SubmitApplication(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationsResponse GetApplications(GetApplicationsRequest request
			)
		{
			YarnServiceProtos.GetApplicationsRequestProto requestProto = ((GetApplicationsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetApplicationsResponsePBImpl(proxy.GetApplications(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetClusterNodesResponse GetClusterNodes(GetClusterNodesRequest request
			)
		{
			YarnServiceProtos.GetClusterNodesRequestProto requestProto = ((GetClusterNodesRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetClusterNodesResponsePBImpl(proxy.GetClusterNodes(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetQueueInfoResponse GetQueueInfo(GetQueueInfoRequest request)
		{
			YarnServiceProtos.GetQueueInfoRequestProto requestProto = ((GetQueueInfoRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetQueueInfoResponsePBImpl(proxy.GetQueueInfo(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetQueueUserAclsInfoResponse GetQueueUserAcls(GetQueueUserAclsInfoRequest
			 request)
		{
			YarnServiceProtos.GetQueueUserAclsInfoRequestProto requestProto = ((GetQueueUserAclsInfoRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetQueueUserAclsInfoResponsePBImpl(proxy.GetQueueUserAcls(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
			 request)
		{
			SecurityProtos.GetDelegationTokenRequestProto requestProto = ((GetDelegationTokenRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetDelegationTokenResponsePBImpl(proxy.GetDelegationToken(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
			 request)
		{
			SecurityProtos.RenewDelegationTokenRequestProto requestProto = ((RenewDelegationTokenRequestPBImpl
				)request).GetProto();
			try
			{
				return new RenewDelegationTokenResponsePBImpl(proxy.RenewDelegationToken(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
			 request)
		{
			SecurityProtos.CancelDelegationTokenRequestProto requestProto = ((CancelDelegationTokenRequestPBImpl
				)request).GetProto();
			try
			{
				return new CancelDelegationTokenResponsePBImpl(proxy.CancelDelegationToken(null, 
					requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual MoveApplicationAcrossQueuesResponse MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
			 request)
		{
			YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto requestProto = ((MoveApplicationAcrossQueuesRequestPBImpl
				)request).GetProto();
			try
			{
				return new MoveApplicationAcrossQueuesResponsePBImpl(proxy.MoveApplicationAcrossQueues
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationAttemptReportResponse GetApplicationAttemptReport(GetApplicationAttemptReportRequest
			 request)
		{
			YarnServiceProtos.GetApplicationAttemptReportRequestProto requestProto = ((GetApplicationAttemptReportRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetApplicationAttemptReportResponsePBImpl(proxy.GetApplicationAttemptReport
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationAttemptsResponse GetApplicationAttempts(GetApplicationAttemptsRequest
			 request)
		{
			YarnServiceProtos.GetApplicationAttemptsRequestProto requestProto = ((GetApplicationAttemptsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetApplicationAttemptsResponsePBImpl(proxy.GetApplicationAttempts(null
					, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainerReportResponse GetContainerReport(GetContainerReportRequest
			 request)
		{
			YarnServiceProtos.GetContainerReportRequestProto requestProto = ((GetContainerReportRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetContainerReportResponsePBImpl(proxy.GetContainerReport(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainersResponse GetContainers(GetContainersRequest request)
		{
			YarnServiceProtos.GetContainersRequestProto requestProto = ((GetContainersRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetContainersResponsePBImpl(proxy.GetContainers(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest
			 request)
		{
			YarnServiceProtos.ReservationSubmissionRequestProto requestProto = ((ReservationSubmissionRequestPBImpl
				)request).GetProto();
			try
			{
				return new ReservationSubmissionResponsePBImpl(proxy.SubmitReservation(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest
			 request)
		{
			YarnServiceProtos.ReservationUpdateRequestProto requestProto = ((ReservationUpdateRequestPBImpl
				)request).GetProto();
			try
			{
				return new ReservationUpdateResponsePBImpl(proxy.UpdateReservation(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest
			 request)
		{
			YarnServiceProtos.ReservationDeleteRequestProto requestProto = ((ReservationDeleteRequestPBImpl
				)request).GetProto();
			try
			{
				return new ReservationDeleteResponsePBImpl(proxy.DeleteReservation(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetNodesToLabelsResponse GetNodeToLabels(GetNodesToLabelsRequest request
			)
		{
			YarnServiceProtos.GetNodesToLabelsRequestProto requestProto = ((GetNodesToLabelsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetNodesToLabelsResponsePBImpl(proxy.GetNodeToLabels(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetLabelsToNodesResponse GetLabelsToNodes(GetLabelsToNodesRequest 
			request)
		{
			YarnServiceProtos.GetLabelsToNodesRequestProto requestProto = ((GetLabelsToNodesRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetLabelsToNodesResponsePBImpl(proxy.GetLabelsToNodes(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetClusterNodeLabelsResponse GetClusterNodeLabels(GetClusterNodeLabelsRequest
			 request)
		{
			YarnServiceProtos.GetClusterNodeLabelsRequestProto requestProto = ((GetClusterNodeLabelsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetClusterNodeLabelsResponsePBImpl(proxy.GetClusterNodeLabels(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
