using System.Net;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api
{
	public interface MRClientProtocol
	{
		/// <summary>Address to which the client is connected</summary>
		/// <returns>InetSocketAddress</returns>
		IPEndPoint GetConnectAddress();

		/// <exception cref="System.IO.IOException"/>
		GetJobReportResponse GetJobReport(GetJobReportRequest request);

		/// <exception cref="System.IO.IOException"/>
		GetTaskReportResponse GetTaskReport(GetTaskReportRequest request);

		/// <exception cref="System.IO.IOException"/>
		GetTaskAttemptReportResponse GetTaskAttemptReport(GetTaskAttemptReportRequest request
			);

		/// <exception cref="System.IO.IOException"/>
		GetCountersResponse GetCounters(GetCountersRequest request);

		/// <exception cref="System.IO.IOException"/>
		GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents(GetTaskAttemptCompletionEventsRequest
			 request);

		/// <exception cref="System.IO.IOException"/>
		GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request);

		/// <exception cref="System.IO.IOException"/>
		GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request);

		/// <exception cref="System.IO.IOException"/>
		KillJobResponse KillJob(KillJobRequest request);

		/// <exception cref="System.IO.IOException"/>
		KillTaskResponse KillTask(KillTaskRequest request);

		/// <exception cref="System.IO.IOException"/>
		KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request);

		/// <exception cref="System.IO.IOException"/>
		FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request);

		/// <exception cref="System.IO.IOException"/>
		GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest request);

		/// <summary>Renew an existing delegation token.</summary>
		/// <param name="request">the delegation token to be renewed.</param>
		/// <returns>the new expiry time for the delegation token.</returns>
		/// <exception cref="System.IO.IOException"/>
		RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest request
			);

		/// <summary>Cancel an existing delegation token.</summary>
		/// <param name="request">the delegation token to be cancelled.</param>
		/// <returns>an empty response.</returns>
		/// <exception cref="System.IO.IOException"/>
		CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest 
			request);
	}
}
