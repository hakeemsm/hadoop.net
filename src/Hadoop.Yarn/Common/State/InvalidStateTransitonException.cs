using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	[System.Serializable]
	public class InvalidStateTransitonException : YarnRuntimeException
	{
		private const long serialVersionUID = 8610511635996283691L;

		private Enum<object> currentState;

		private Enum<object> @event;

		public InvalidStateTransitonException(Enum<object> currentState, Enum<object> @event
			)
			: base("Invalid event: " + @event + " at " + currentState)
		{
			this.currentState = currentState;
			this.@event = @event;
		}

		public virtual Enum<object> GetCurrentState()
		{
			return currentState;
		}

		public virtual Enum<object> GetEvent()
		{
			return @event;
		}
	}
}
