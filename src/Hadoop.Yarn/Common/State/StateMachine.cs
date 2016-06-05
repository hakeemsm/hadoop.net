using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	public interface StateMachine<State, Eventtype, Event>
		where State : Enum<STATE>
		where Eventtype : Enum<EVENTTYPE>
	{
		STATE GetCurrentState();

		/// <exception cref="Org.Apache.Hadoop.Yarn.State.InvalidStateTransitonException"/>
		STATE DoTransition(EVENTTYPE eventType, EVENT @event);
	}
}
