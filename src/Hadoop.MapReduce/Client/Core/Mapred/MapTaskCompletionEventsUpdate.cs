using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A class that represents the communication between the tasktracker and child
	/// tasks w.r.t the map task completion events.
	/// </summary>
	/// <remarks>
	/// A class that represents the communication between the tasktracker and child
	/// tasks w.r.t the map task completion events. It also indicates whether the
	/// child task should reset its events index.
	/// </remarks>
	public class MapTaskCompletionEventsUpdate : Writable
	{
		internal TaskCompletionEvent[] events;

		internal bool reset;

		public MapTaskCompletionEventsUpdate()
		{
		}

		public MapTaskCompletionEventsUpdate(TaskCompletionEvent[] events, bool reset)
		{
			this.events = events;
			this.reset = reset;
		}

		public virtual bool ShouldReset()
		{
			return reset;
		}

		public virtual TaskCompletionEvent[] GetMapTaskCompletionEvents()
		{
			return events;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteBoolean(reset);
			@out.WriteInt(events.Length);
			foreach (TaskCompletionEvent @event in events)
			{
				@event.Write(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			reset = @in.ReadBoolean();
			events = new TaskCompletionEvent[@in.ReadInt()];
			for (int i = 0; i < events.Length; ++i)
			{
				events[i] = new TaskCompletionEvent();
				events[i].ReadFields(@in);
			}
		}
	}
}
