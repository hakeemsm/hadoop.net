using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	public abstract class TimelineStore : Org.Apache.Hadoop.Service.Service, TimelineReader
		, TimelineWriter
	{
		/// <summary>
		/// The system filter which will be automatically added to a
		/// <see cref="TimelineEntity"/>
		/// 's primary filter section when storing the entity.
		/// The filter key is case sensitive. Users are supposed not to use the key
		/// reserved by the timeline system.
		/// </summary>
		public enum SystemFilter
		{
			EntityOwner
		}
	}

	public static class TimelineStoreConstants
	{
	}
}
