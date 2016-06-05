using System;
using System.IO;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Impl.PB.Client
{
	public class MRClientProtocolPBClientImpl : MRClientProtocol, IDisposable
	{
		protected internal MRClientProtocolPB proxy;

		public MRClientProtocolPBClientImpl()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MRClientProtocolPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(MRClientProtocolPB), typeof(ProtobufRpcEngine)
				);
			proxy = RPC.GetProxy<MRClientProtocolPB>(clientVersion, addr, conf);
		}

		public virtual IPEndPoint GetConnectAddress()
		{
			return RPC.GetServerAddress(proxy);
		}

		public virtual void Close()
		{
			if (this.proxy != null)
			{
				RPC.StopProxy(this.proxy);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetJobReportResponse GetJobReport(GetJobReportRequest request)
		{
			MRServiceProtos.GetJobReportRequestProto requestProto = ((GetJobReportRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetJobReportResponsePBImpl(proxy.GetJobReport(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskReportResponse GetTaskReport(GetTaskReportRequest request)
		{
			MRServiceProtos.GetTaskReportRequestProto requestProto = ((GetTaskReportRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetTaskReportResponsePBImpl(proxy.GetTaskReport(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskAttemptReportResponse GetTaskAttemptReport(GetTaskAttemptReportRequest
			 request)
		{
			MRServiceProtos.GetTaskAttemptReportRequestProto requestProto = ((GetTaskAttemptReportRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetTaskAttemptReportResponsePBImpl(proxy.GetTaskAttemptReport(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetCountersResponse GetCounters(GetCountersRequest request)
		{
			MRServiceProtos.GetCountersRequestProto requestProto = ((GetCountersRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetCountersResponsePBImpl(proxy.GetCounters(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents
			(GetTaskAttemptCompletionEventsRequest request)
		{
			MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto requestProto = ((GetTaskAttemptCompletionEventsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetTaskAttemptCompletionEventsResponsePBImpl(proxy.GetTaskAttemptCompletionEvents
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request
			)
		{
			MRServiceProtos.GetTaskReportsRequestProto requestProto = ((GetTaskReportsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetTaskReportsResponsePBImpl(proxy.GetTaskReports(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request
			)
		{
			MRServiceProtos.GetDiagnosticsRequestProto requestProto = ((GetDiagnosticsRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetDiagnosticsResponsePBImpl(proxy.GetDiagnostics(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

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
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual KillJobResponse KillJob(KillJobRequest request)
		{
			MRServiceProtos.KillJobRequestProto requestProto = ((KillJobRequestPBImpl)request
				).GetProto();
			try
			{
				return new KillJobResponsePBImpl(proxy.KillJob(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual KillTaskResponse KillTask(KillTaskRequest request)
		{
			MRServiceProtos.KillTaskRequestProto requestProto = ((KillTaskRequestPBImpl)request
				).GetProto();
			try
			{
				return new KillTaskResponsePBImpl(proxy.KillTask(null, requestProto));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request
			)
		{
			MRServiceProtos.KillTaskAttemptRequestProto requestProto = ((KillTaskAttemptRequestPBImpl
				)request).GetProto();
			try
			{
				return new KillTaskAttemptResponsePBImpl(proxy.KillTaskAttempt(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request
			)
		{
			MRServiceProtos.FailTaskAttemptRequestProto requestProto = ((FailTaskAttemptRequestPBImpl
				)request).GetProto();
			try
			{
				return new FailTaskAttemptResponsePBImpl(proxy.FailTaskAttempt(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				throw UnwrapAndThrowException(e);
			}
		}

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
				throw UnwrapAndThrowException(e);
			}
		}

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
				throw UnwrapAndThrowException(e);
			}
		}

		private IOException UnwrapAndThrowException(ServiceException se)
		{
			if (se.InnerException is RemoteException)
			{
				return ((RemoteException)se.InnerException).UnwrapRemoteException();
			}
			else
			{
				if (se.InnerException is IOException)
				{
					return (IOException)se.InnerException;
				}
				else
				{
					throw new UndeclaredThrowableException(se.InnerException);
				}
			}
		}
	}
}
