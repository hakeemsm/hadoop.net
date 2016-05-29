using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	/// <summary>Event Dispatcher interface.</summary>
	/// <remarks>
	/// Event Dispatcher interface. It dispatches events to registered
	/// event handlers based on event types.
	/// </remarks>
	public abstract class Dispatcher
	{
		public const string DispatcherExitOnErrorKey = "yarn.dispatcher.exit-on-error";

		public const bool DefaultDispatcherExitOnError = false;

		// Configuration to make sure dispatcher crashes but doesn't do system-exit in
		// case of errors. By default, it should be false, so that tests are not
		// affected. For all daemons it should be explicitly set to true so that
		// daemons can crash instead of hanging around.
		public abstract EventHandler GetEventHandler();

		public abstract void Register(Type eventType, EventHandler handler);
	}

	public static class DispatcherConstants
	{
	}
}
