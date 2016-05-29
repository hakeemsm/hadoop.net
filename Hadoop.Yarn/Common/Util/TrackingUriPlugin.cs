using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>Plugin to derive a tracking URL from a Yarn Application ID</summary>
	public abstract class TrackingUriPlugin : Configured
	{
		/// <summary>Given an application ID, return a tracking URI.</summary>
		/// <param name="id">the ID for which a URI is returned</param>
		/// <returns>the tracking URI</returns>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public abstract URI GetTrackingUri(ApplicationId id);
	}
}
