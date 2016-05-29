using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ApplicationAttemptReport</c>
	/// is a report of an application attempt.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="ApplicationAttemptId"/>
	/// of the application.</li>
	/// <li>Host on which the <code>ApplicationMaster</code> of this attempt is
	/// running.</li>
	/// <li>RPC port of the <code>ApplicationMaster</code> of this attempt.</li>
	/// <li>Tracking URL.</li>
	/// <li>Diagnostic information in case of errors.</li>
	/// <li>
	/// <see cref="YarnApplicationAttemptState"/>
	/// of the application attempt.</li>
	/// <li>
	/// <see cref="ContainerId"/>
	/// of the master Container.</li>
	/// </ul>
	/// </summary>
	public abstract class ApplicationAttemptReport
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ApplicationAttemptReport NewInstance(ApplicationAttemptId applicationAttemptId
			, string host, int rpcPort, string url, string oUrl, string diagnostics, YarnApplicationAttemptState
			 state, ContainerId amContainerId)
		{
			ApplicationAttemptReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationAttemptReport
				>();
			report.SetApplicationAttemptId(applicationAttemptId);
			report.SetHost(host);
			report.SetRpcPort(rpcPort);
			report.SetTrackingUrl(url);
			report.SetOriginalTrackingUrl(oUrl);
			report.SetDiagnostics(diagnostics);
			report.SetYarnApplicationAttemptState(state);
			report.SetAMContainerId(amContainerId);
			return report;
		}

		/// <summary>Get the <em>YarnApplicationAttemptState</em> of the application attempt.
		/// 	</summary>
		/// <returns><em>YarnApplicationAttemptState</em> of the application attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract YarnApplicationAttemptState GetYarnApplicationAttemptState();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetYarnApplicationAttemptState(YarnApplicationAttemptState yarnApplicationAttemptState
			);

		/// <summary>Get the <em>RPC port</em> of this attempt <code>ApplicationMaster</code>.
		/// 	</summary>
		/// <returns><em>RPC port</em> of this attempt <code>ApplicationMaster</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetRpcPort();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetRpcPort(int rpcPort);

		/// <summary>
		/// Get the <em>host</em> on which this attempt of
		/// <code>ApplicationMaster</code> is running.
		/// </summary>
		/// <returns>
		/// <em>host</em> on which this attempt of
		/// <code>ApplicationMaster</code> is running
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetHost();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetHost(string host);

		/// <summary>
		/// Get the <em>diagnositic information</em> of the application attempt in case
		/// of errors.
		/// </summary>
		/// <returns>
		/// <em>diagnositic information</em> of the application attempt in case
		/// of errors
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetDiagnostics();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetDiagnostics(string diagnostics);

		/// <summary>Get the <em>tracking url</em> for the application attempt.</summary>
		/// <returns><em>tracking url</em> for the application attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetTrackingUrl();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetTrackingUrl(string url);

		/// <summary>Get the <em>original tracking url</em> for the application attempt.</summary>
		/// <returns><em>original tracking url</em> for the application attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetOriginalTrackingUrl();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetOriginalTrackingUrl(string oUrl);

		/// <summary>
		/// Get the <code>ApplicationAttemptId</code> of this attempt of the
		/// application
		/// </summary>
		/// <returns><code>ApplicationAttemptId</code> of the attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationAttemptId GetApplicationAttemptId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			);

		/// <summary>Get the <code>ContainerId</code> of AMContainer for this attempt</summary>
		/// <returns><code>ContainerId</code> of the attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerId GetAMContainerId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAMContainerId(ContainerId amContainerId);
	}
}
