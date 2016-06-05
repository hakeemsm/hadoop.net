using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	/// <summary>Hook for Transition.</summary>
	/// <remarks>
	/// Hook for Transition. This lead to state machine to move to
	/// the post state as registered in the state machine.
	/// </remarks>
	public interface SingleArcTransition<Operand, Event>
	{
		/// <summary>Transition hook.</summary>
		/// <param name="operand">
		/// the entity attached to the FSM, whose internal
		/// state may change.
		/// </param>
		/// <param name="event">causal event</param>
		void Transition(OPERAND operand, EVENT @event);
	}
}
