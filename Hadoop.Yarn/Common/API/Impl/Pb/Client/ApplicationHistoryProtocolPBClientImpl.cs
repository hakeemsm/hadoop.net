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
	public class ApplicationHistoryProtocolPBClientImpl : ApplicationHistoryProtocol, 
		IDisposable
	{
		private ApplicationHistoryProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ApplicationHistoryProtocolPBClientImpl(long clientVersion, IPEndPoint addr
			, Configuration conf)
		{
			RPC.SetProtocolEngine(conf, typeof(ApplicationHistoryProtocolPB), typeof(ProtobufRpcEngine
				));
			proxy = RPC.GetProxy<ApplicationHistoryProtocolPB>(clientVersion, addr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (this.proxy != null)
			{
				RPC.StopProxy(this.proxy);
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
	}
}
