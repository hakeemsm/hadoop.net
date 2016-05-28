using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class CommitterEvent : AbstractEvent<CommitterEventType>
	{
		public CommitterEvent(CommitterEventType type)
			: base(type)
		{
		}
	}
}
