using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	/// <summary>Interface for handling events of type T</summary>
	/// <?/>
	public interface EventHandler<T>
		where T : Org.Apache.Hadoop.Yarn.Event.Event
	{
		void Handle(T @event);
	}
}
