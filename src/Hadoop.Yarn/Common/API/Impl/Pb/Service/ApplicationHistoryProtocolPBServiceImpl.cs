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
	public class ApplicationHistoryProtocolPBServiceImpl : ApplicationHistoryProtocolPB
	{
		private ApplicationHistoryProtocol real;

		public ApplicationHistoryProtocolPBServiceImpl(ApplicationHistoryProtocol impl)
		{
			this.real = impl;
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
	}
}
