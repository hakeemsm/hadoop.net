using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ApplicationReport</c>
	/// is a report of an application.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="ApplicationId"/>
	/// of the application.</li>
	/// <li>Applications user.</li>
	/// <li>Application queue.</li>
	/// <li>Application name.</li>
	/// <li>Host on which the <code>ApplicationMaster</code> is running.</li>
	/// <li>RPC port of the <code>ApplicationMaster</code>.</li>
	/// <li>Tracking URL.</li>
	/// <li>
	/// <see cref="YarnApplicationState"/>
	/// of the application.</li>
	/// <li>Diagnostic information in case of errors.</li>
	/// <li>Start time of the application.</li>
	/// <li>Client
	/// <see cref="Token"/>
	/// of the application (if security is enabled).</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest)
	/// 	"/>
	public abstract class ApplicationReport
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ApplicationReport NewInstance(ApplicationId applicationId, ApplicationAttemptId
			 applicationAttemptId, string user, string queue, string name, string host, int 
			rpcPort, Token clientToAMToken, YarnApplicationState state, string diagnostics, 
			string url, long startTime, long finishTime, FinalApplicationStatus finalStatus, 
			ApplicationResourceUsageReport appResources, string origTrackingUrl, float progress
			, string applicationType, Token amRmToken)
		{
			ApplicationReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationReport
				>();
			report.SetApplicationId(applicationId);
			report.SetCurrentApplicationAttemptId(applicationAttemptId);
			report.SetUser(user);
			report.SetQueue(queue);
			report.SetName(name);
			report.SetHost(host);
			report.SetRpcPort(rpcPort);
			report.SetClientToAMToken(clientToAMToken);
			report.SetYarnApplicationState(state);
			report.SetDiagnostics(diagnostics);
			report.SetTrackingUrl(url);
			report.SetStartTime(startTime);
			report.SetFinishTime(finishTime);
			report.SetFinalApplicationStatus(finalStatus);
			report.SetApplicationResourceUsageReport(appResources);
			report.SetOriginalTrackingUrl(origTrackingUrl);
			report.SetProgress(progress);
			report.SetApplicationType(applicationType);
			report.SetAMRMToken(amRmToken);
			return report;
		}

		/// <summary>Get the <code>ApplicationId</code> of the application.</summary>
		/// <returns><code>ApplicationId</code> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationId GetApplicationId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationId(ApplicationId applicationId);

		/// <summary>
		/// Get the <code>ApplicationAttemptId</code> of the current
		/// attempt of the application
		/// </summary>
		/// <returns><code>ApplicationAttemptId</code> of the attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationAttemptId GetCurrentApplicationAttemptId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetCurrentApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			);

		/// <summary>Get the <em>user</em> who submitted the application.</summary>
		/// <returns><em>user</em> who submitted the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetUser();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUser(string user);

		/// <summary>Get the <em>queue</em> to which the application was submitted.</summary>
		/// <returns><em>queue</em> to which the application was submitted</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetQueue();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetQueue(string queue);

		/// <summary>Get the user-defined <em>name</em> of the application.</summary>
		/// <returns><em>name</em> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetName();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetName(string name);

		/// <summary>
		/// Get the <em>host</em> on which the <code>ApplicationMaster</code>
		/// is running.
		/// </summary>
		/// <returns>
		/// <em>host</em> on which the <code>ApplicationMaster</code>
		/// is running
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetHost();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetHost(string host);

		/// <summary>Get the <em>RPC port</em> of the <code>ApplicationMaster</code>.</summary>
		/// <returns><em>RPC port</em> of the <code>ApplicationMaster</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetRpcPort();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetRpcPort(int rpcPort);

		/// <summary>
		/// Get the <em>client token</em> for communicating with the
		/// <code>ApplicationMaster</code>.
		/// </summary>
		/// <remarks>
		/// Get the <em>client token</em> for communicating with the
		/// <code>ApplicationMaster</code>.
		/// <p>
		/// <em>ClientToAMToken</em> is the security token used by the AMs to verify
		/// authenticity of any <code>client</code>.
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code>, provides a secure token (via
		/// <see cref="GetClientToAMToken()"/>
		/// ) which is verified by the
		/// ApplicationMaster when the client directly talks to an AM.
		/// </p>
		/// </remarks>
		/// <returns>
		/// <em>client token</em> for communicating with the
		/// <code>ApplicationMaster</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Token GetClientToAMToken();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetClientToAMToken(Token clientToAMToken);

		/// <summary>Get the <code>YarnApplicationState</code> of the application.</summary>
		/// <returns><code>YarnApplicationState</code> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract YarnApplicationState GetYarnApplicationState();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetYarnApplicationState(YarnApplicationState state);

		/// <summary>
		/// Get  the <em>diagnositic information</em> of the application in case of
		/// errors.
		/// </summary>
		/// <returns>
		/// <em>diagnositic information</em> of the application in case
		/// of errors
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetDiagnostics();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetDiagnostics(string diagnostics);

		/// <summary>Get the <em>tracking url</em> for the application.</summary>
		/// <returns><em>tracking url</em> for the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetTrackingUrl();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetTrackingUrl(string url);

		/// <summary>Get the original not-proxied <em>tracking url</em> for the application.</summary>
		/// <remarks>
		/// Get the original not-proxied <em>tracking url</em> for the application.
		/// This is intended to only be used by the proxy itself.
		/// </remarks>
		/// <returns>the original not-proxied <em>tracking url</em> for the application</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract string GetOriginalTrackingUrl();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetOriginalTrackingUrl(string url);

		/// <summary>Get the <em>start time</em> of the application.</summary>
		/// <returns><em>start time</em> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetStartTime();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetStartTime(long startTime);

		/// <summary>Get the <em>finish time</em> of the application.</summary>
		/// <returns><em>finish time</em> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetFinishTime();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetFinishTime(long finishTime);

		/// <summary>Get the <em>final finish status</em> of the application.</summary>
		/// <returns><em>final finish status</em> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract FinalApplicationStatus GetFinalApplicationStatus();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetFinalApplicationStatus(FinalApplicationStatus finishState
			);

		/// <summary>Retrieve the structure containing the job resources for this application
		/// 	</summary>
		/// <returns>the job resources structure for this application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationResourceUsageReport GetApplicationResourceUsageReport(
			);

		/// <summary>Store the structure containing the job resources for this application</summary>
		/// <param name="appResources">structure for this application</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationResourceUsageReport(ApplicationResourceUsageReport
			 appResources);

		/// <summary>Get the application's progress ( range 0.0 to 1.0 )</summary>
		/// <returns>application's progress</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract float GetProgress();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetProgress(float progress);

		/// <summary>Get the application's Type</summary>
		/// <returns>application's Type</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetApplicationType();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationType(string applicationType);

		/// <summary>Get all tags corresponding to the application</summary>
		/// <returns>Application's tags</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ICollection<string> GetApplicationTags();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationTags(ICollection<string> tags);

		[InterfaceAudience.Private]
		[InterfaceStability.Stable]
		public abstract void SetAMRMToken(Token amRmToken);

		/// <summary>Get the AMRM token of the application.</summary>
		/// <remarks>
		/// Get the AMRM token of the application.
		/// <p>
		/// The AMRM token is required for AM to RM scheduling operations. For
		/// managed Application Masters Yarn takes care of injecting it. For unmanaged
		/// Applications Masters, the token must be obtained via this method and set
		/// in the
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// of the
		/// current user.
		/// <p>
		/// The AMRM token will be returned only if all the following conditions are
		/// met:
		/// <ul>
		/// <li>the requester is the owner of the ApplicationMaster</li>
		/// <li>the application master is an unmanaged ApplicationMaster</li>
		/// <li>the application master is in ACCEPTED state</li>
		/// </ul>
		/// Else this method returns NULL.
		/// </remarks>
		/// <returns>the AM to RM token if available.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Token GetAMRMToken();
	}
}
