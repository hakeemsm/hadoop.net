using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public interface HistoryEventHandler
	{
		/// <exception cref="System.IO.IOException"/>
		void HandleEvent(HistoryEvent @event);
	}
}
