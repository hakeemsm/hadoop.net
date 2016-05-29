using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	/// <summary>Hook for Transition.</summary>
	/// <remarks>
	/// Hook for Transition.
	/// Post state is decided by Transition hook. Post state must be one of the
	/// valid post states registered in StateMachine.
	/// </remarks>
	public interface MultipleArcTransition<Operand, Event, State>
		where State : Enum<STATE>
	{
		/// <summary>Transition hook.</summary>
		/// <returns>
		/// the postState. Post state must be one of the
		/// valid post states registered in StateMachine.
		/// </returns>
		/// <param name="operand">
		/// the entity attached to the FSM, whose internal
		/// state may change.
		/// </param>
		/// <param name="event">causal event</param>
		STATE Transition(OPERAND operand, EVENT @event);
	}
}
