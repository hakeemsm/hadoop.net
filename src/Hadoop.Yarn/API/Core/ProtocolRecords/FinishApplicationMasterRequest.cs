using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The finalization request sent by the
	/// <c>ApplicationMaster</c>
	/// to
	/// inform the
	/// <c>ResourceManager</c>
	/// about its completion.
	/// <p>
	/// The final request includes details such:
	/// <ul>
	/// <li>Final state of the
	/// <c>ApplicationMaster</c>
	/// </li>
	/// <li>
	/// Diagnostic information in case of failure of the
	/// <c>ApplicationMaster</c>
	/// </li>
	/// <li>Tracking URL</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.FinishApplicationMaster(FinishApplicationMasterRequest)
	/// 	"/>
	public abstract class FinishApplicationMasterRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static FinishApplicationMasterRequest NewInstance(FinalApplicationStatus finalAppStatus
			, string diagnostics, string url)
		{
			FinishApplicationMasterRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<FinishApplicationMasterRequest>();
			request.SetFinalApplicationStatus(finalAppStatus);
			request.SetDiagnostics(diagnostics);
			request.SetTrackingUrl(url);
			return request;
		}

		/// <summary>Get <em>final state</em> of the <code>ApplicationMaster</code>.</summary>
		/// <returns><em>final state</em> of the <code>ApplicationMaster</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract FinalApplicationStatus GetFinalApplicationStatus();

		/// <summary>Set the <em>final state</em> of the <code>ApplicationMaster</code></summary>
		/// <param name="finalState"><em>final state</em> of the <code>ApplicationMaster</code>
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetFinalApplicationStatus(FinalApplicationStatus finalState);

		/// <summary>Get <em>diagnostic information</em> on application failure.</summary>
		/// <returns><em>diagnostic information</em> on application failure</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetDiagnostics();

		/// <summary>Set <em>diagnostic information</em> on application failure.</summary>
		/// <param name="diagnostics"><em>diagnostic information</em> on application failure</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetDiagnostics(string diagnostics);

		/// <summary>Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.</summary>
		/// <remarks>
		/// Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
		/// This url if contains scheme then that will be used by resource manager
		/// web application proxy otherwise it will default to http.
		/// </remarks>
		/// <returns><em>tracking URL</em>for the <code>ApplicationMaster</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetTrackingUrl();

		/// <summary>Set the <em>final tracking URL</em>for the <code>ApplicationMaster</code>.
		/// 	</summary>
		/// <remarks>
		/// Set the <em>final tracking URL</em>for the <code>ApplicationMaster</code>.
		/// This is the web-URL to which ResourceManager or web-application proxy will
		/// redirect client/users once the application is finished and the
		/// <code>ApplicationMaster</code> is gone.
		/// <p>
		/// If the passed url has a scheme then that will be used by the
		/// ResourceManager and web-application proxy, otherwise the scheme will
		/// default to http.
		/// </p>
		/// <p>
		/// Empty, null, "N/A" strings are all valid besides a real URL. In case an url
		/// isn't explicitly passed, it defaults to "N/A" on the ResourceManager.
		/// <p>
		/// </remarks>
		/// <param name="url"><em>tracking URL</em>for the <code>ApplicationMaster</code></param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetTrackingUrl(string url);
	}
}
