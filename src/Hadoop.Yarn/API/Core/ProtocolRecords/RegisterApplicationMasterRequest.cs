using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The request sent by the
	/// <c>ApplicationMaster</c>
	/// to
	/// <c>ResourceManager</c>
	/// on registration.
	/// <p>
	/// The registration includes details such as:
	/// <ul>
	/// <li>Hostname on which the AM is running.</li>
	/// <li>RPC Port</li>
	/// <li>Tracking URL</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.RegisterApplicationMaster(RegisterApplicationMasterRequest)
	/// 	"/>
	public abstract class RegisterApplicationMasterRequest
	{
		/// <summary>Create a new instance of <code>RegisterApplicationMasterRequest</code>.</summary>
		/// <remarks>
		/// Create a new instance of <code>RegisterApplicationMasterRequest</code>.
		/// If <em>port, trackingUrl</em> is not used, use the following default value:
		/// <ul>
		/// <li>port: -1</li>
		/// <li>trackingUrl: null</li>
		/// </ul>
		/// The port is allowed to be any integer larger than or equal to -1.
		/// </remarks>
		/// <returns>the new instance of <code>RegisterApplicationMasterRequest</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RegisterApplicationMasterRequest NewInstance(string host, int port, 
			string trackingUrl)
		{
			RegisterApplicationMasterRequest request = Records.NewRecord<RegisterApplicationMasterRequest
				>();
			request.SetHost(host);
			request.SetRpcPort(port);
			request.SetTrackingUrl(trackingUrl);
			return request;
		}

		/// <summary>
		/// Get the <em>host</em> on which the <code>ApplicationMaster</code> is
		/// running.
		/// </summary>
		/// <returns><em>host</em> on which the <code>ApplicationMaster</code> is running</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetHost();

		/// <summary>
		/// Set the <em>host</em> on which the <code>ApplicationMaster</code> is
		/// running.
		/// </summary>
		/// <param name="host">
		/// <em>host</em> on which the <code>ApplicationMaster</code>
		/// is running
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetHost(string host);

		/// <summary>
		/// Get the <em>RPC port</em> on which the
		/// <c>ApplicationMaster</c>
		/// is
		/// responding.
		/// </summary>
		/// <returns>
		/// the <em>RPC port</em> on which the
		/// <c>ApplicationMaster</c>
		/// is responding
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetRpcPort();

		/// <summary>
		/// Set the <em>RPC port</em> on which the
		/// <c>ApplicationMaster</c>
		/// is
		/// responding.
		/// </summary>
		/// <param name="port">
		/// <em>RPC port</em> on which the
		/// <c>ApplicationMaster</c>
		/// is responding
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetRpcPort(int port);

		/// <summary>Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.</summary>
		/// <remarks>
		/// Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
		/// This url if contains scheme then that will be used by resource manager
		/// web application proxy otherwise it will default to http.
		/// </remarks>
		/// <returns><em>tracking URL</em> for the <code>ApplicationMaster</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetTrackingUrl();

		/// <summary>
		/// Set the <em>tracking URL</em>for the <code>ApplicationMaster</code> while
		/// it is running.
		/// </summary>
		/// <remarks>
		/// Set the <em>tracking URL</em>for the <code>ApplicationMaster</code> while
		/// it is running. This is the web-URL to which ResourceManager or
		/// web-application proxy will redirect client/users while the application and
		/// the <code>ApplicationMaster</code> are still running.
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
		/// <param name="trackingUrl"><em>tracking URL</em>for the <code>ApplicationMaster</code>
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetTrackingUrl(string trackingUrl);
	}
}
