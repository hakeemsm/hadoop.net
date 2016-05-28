using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Impl.PB.Service
{
	public class MRClientProtocolPBServiceImpl : MRClientProtocolPB
	{
		private MRClientProtocol real;

		public MRClientProtocolPBServiceImpl(MRClientProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetJobReportResponseProto GetJobReport(RpcController
			 controller, MRServiceProtos.GetJobReportRequestProto proto)
		{
			GetJobReportRequestPBImpl request = new GetJobReportRequestPBImpl(proto);
			try
			{
				GetJobReportResponse response = real.GetJobReport(request);
				return ((GetJobReportResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetTaskReportResponseProto GetTaskReport(RpcController
			 controller, MRServiceProtos.GetTaskReportRequestProto proto)
		{
			GetTaskReportRequest request = new GetTaskReportRequestPBImpl(proto);
			try
			{
				GetTaskReportResponse response = real.GetTaskReport(request);
				return ((GetTaskReportResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetTaskAttemptReportResponseProto GetTaskAttemptReport
			(RpcController controller, MRServiceProtos.GetTaskAttemptReportRequestProto proto
			)
		{
			GetTaskAttemptReportRequest request = new GetTaskAttemptReportRequestPBImpl(proto
				);
			try
			{
				GetTaskAttemptReportResponse response = real.GetTaskAttemptReport(request);
				return ((GetTaskAttemptReportResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetCountersResponseProto GetCounters(RpcController
			 controller, MRServiceProtos.GetCountersRequestProto proto)
		{
			GetCountersRequest request = new GetCountersRequestPBImpl(proto);
			try
			{
				GetCountersResponse response = real.GetCounters(request);
				return ((GetCountersResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto GetTaskAttemptCompletionEvents
			(RpcController controller, MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto
			 proto)
		{
			GetTaskAttemptCompletionEventsRequest request = new GetTaskAttemptCompletionEventsRequestPBImpl
				(proto);
			try
			{
				GetTaskAttemptCompletionEventsResponse response = real.GetTaskAttemptCompletionEvents
					(request);
				return ((GetTaskAttemptCompletionEventsResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetTaskReportsResponseProto GetTaskReports(RpcController
			 controller, MRServiceProtos.GetTaskReportsRequestProto proto)
		{
			GetTaskReportsRequest request = new GetTaskReportsRequestPBImpl(proto);
			try
			{
				GetTaskReportsResponse response = real.GetTaskReports(request);
				return ((GetTaskReportsResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.GetDiagnosticsResponseProto GetDiagnostics(RpcController
			 controller, MRServiceProtos.GetDiagnosticsRequestProto proto)
		{
			GetDiagnosticsRequest request = new GetDiagnosticsRequestPBImpl(proto);
			try
			{
				GetDiagnosticsResponse response = real.GetDiagnostics(request);
				return ((GetDiagnosticsResponsePBImpl)response).GetProto();
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
			GetDelegationTokenRequest request = new GetDelegationTokenRequestPBImpl(proto);
			try
			{
				GetDelegationTokenResponse response = real.GetDelegationToken(request);
				return ((GetDelegationTokenResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.KillJobResponseProto KillJob(RpcController controller
			, MRServiceProtos.KillJobRequestProto proto)
		{
			KillJobRequest request = new KillJobRequestPBImpl(proto);
			try
			{
				KillJobResponse response = real.KillJob(request);
				return ((KillJobResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.KillTaskResponseProto KillTask(RpcController controller
			, MRServiceProtos.KillTaskRequestProto proto)
		{
			KillTaskRequest request = new KillTaskRequestPBImpl(proto);
			try
			{
				KillTaskResponse response = real.KillTask(request);
				return ((KillTaskResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.KillTaskAttemptResponseProto KillTaskAttempt(RpcController
			 controller, MRServiceProtos.KillTaskAttemptRequestProto proto)
		{
			KillTaskAttemptRequest request = new KillTaskAttemptRequestPBImpl(proto);
			try
			{
				KillTaskAttemptResponse response = real.KillTaskAttempt(request);
				return ((KillTaskAttemptResponsePBImpl)response).GetProto();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual MRServiceProtos.FailTaskAttemptResponseProto FailTaskAttempt(RpcController
			 controller, MRServiceProtos.FailTaskAttemptRequestProto proto)
		{
			FailTaskAttemptRequest request = new FailTaskAttemptRequestPBImpl(proto);
			try
			{
				FailTaskAttemptResponse response = real.FailTaskAttempt(request);
				return ((FailTaskAttemptResponsePBImpl)response).GetProto();
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
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
