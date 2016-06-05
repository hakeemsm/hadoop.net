using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>This interface is for storing timeline information.</summary>
	public interface TimelineWriter
	{
		/// <summary>Stores entity information to the timeline store.</summary>
		/// <remarks>
		/// Stores entity information to the timeline store. Any errors occurring for
		/// individual put request objects will be reported in the response.
		/// </remarks>
		/// <param name="data">
		/// a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntities"/>
		/// object.
		/// </param>
		/// <returns>
		/// a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelinePutResponse"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		TimelinePutResponse Put(TimelineEntities data);

		/// <summary>Store domain information to the timeline store.</summary>
		/// <remarks>
		/// Store domain information to the timeline store. If A domain of the
		/// same ID already exists in the timeline store, it will be COMPLETELY updated
		/// with the given domain.
		/// </remarks>
		/// <param name="domain">
		/// a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineDomain"/>
		/// object
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void Put(TimelineDomain domain);
	}
}
