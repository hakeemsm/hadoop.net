using System;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	public class InlineDispatcher : AsyncDispatcher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(InlineDispatcher));

		private class TestEventHandler : EventHandler
		{
			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				this._enclosing.Dispatch(@event);
			}

			internal TestEventHandler(InlineDispatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly InlineDispatcher _enclosing;
		}

		protected internal override void Dispatch(Org.Apache.Hadoop.Yarn.Event.Event @event
			)
		{
			Log.Info("Dispatching the event " + @event.GetType().FullName + "." + @event.ToString
				());
			Type type = @event.GetType().GetDeclaringClass();
			if (eventDispatchers[type] != null)
			{
				eventDispatchers[type].Handle(@event);
			}
		}

		public override EventHandler GetEventHandler()
		{
			return new InlineDispatcher.TestEventHandler(this);
		}

		public class EmptyEventHandler : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event>
		{
			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
			//do nothing      
		}
	}
}
