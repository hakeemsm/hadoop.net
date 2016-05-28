using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Namenode base state to implement state machine pattern.</summary>
	public abstract class HAState
	{
		protected internal readonly HAServiceProtocol.HAServiceState state;

		private long lastHATransitionTime;

		/// <summary>Constructor</summary>
		/// <param name="name">Name of the state.</param>
		public HAState(HAServiceProtocol.HAServiceState state)
		{
			this.state = state;
		}

		/// <returns>the generic service state</returns>
		public virtual HAServiceProtocol.HAServiceState GetServiceState()
		{
			return state;
		}

		/// <summary>Internal method to transition the state of a given namenode to a new state.
		/// 	</summary>
		/// <param name="nn">Namenode</param>
		/// <param name="s">new state</param>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException">on failure to transition to new state.
		/// 	</exception>
		protected internal void SetStateInternal(HAContext context, Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.HAState
			 s)
		{
			PrepareToExitState(context);
			s.PrepareToEnterState(context);
			context.WriteLock();
			try
			{
				ExitState(context);
				context.SetState(s);
				s.EnterState(context);
				s.UpdateLastHATransitionTime();
			}
			finally
			{
				context.WriteUnlock();
			}
		}

		/// <summary>Gets the most recent HA transition time in milliseconds from the epoch.</summary>
		/// <returns>the most recent HA transition time in milliseconds from the epoch.</returns>
		public virtual long GetLastHATransitionTime()
		{
			return lastHATransitionTime;
		}

		private void UpdateLastHATransitionTime()
		{
			lastHATransitionTime = Time.Now();
		}

		/// <summary>Method to be overridden by subclasses to prepare to enter a state.</summary>
		/// <remarks>
		/// Method to be overridden by subclasses to prepare to enter a state.
		/// This method is called <em>without</em> the context being locked,
		/// and after
		/// <see cref="PrepareToExitState(HAContext)"/>
		/// has been called
		/// for the previous state, but before
		/// <see cref="ExitState(HAContext)"/>
		/// has been called for the previous state.
		/// </remarks>
		/// <param name="context">HA context</param>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException">on precondition failure
		/// 	</exception>
		public virtual void PrepareToEnterState(HAContext context)
		{
		}

		/// <summary>
		/// Method to be overridden by subclasses to perform steps necessary for
		/// entering a state.
		/// </summary>
		/// <param name="context">HA context</param>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException">on failure to enter the state.
		/// 	</exception>
		public abstract void EnterState(HAContext context);

		/// <summary>Method to be overridden by subclasses to prepare to exit a state.</summary>
		/// <remarks>
		/// Method to be overridden by subclasses to prepare to exit a state.
		/// This method is called <em>without</em> the context being locked.
		/// This is used by the standby state to cancel any checkpoints
		/// that are going on. It can also be used to check any preconditions
		/// for the state transition.
		/// This method should not make any destructuve changes to the state
		/// (eg stopping threads) since
		/// <see cref="PrepareToEnterState(HAContext)"/>
		/// may subsequently cancel the state transition.
		/// </remarks>
		/// <param name="context">HA context</param>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException">on precondition failure
		/// 	</exception>
		public virtual void PrepareToExitState(HAContext context)
		{
		}

		/// <summary>
		/// Method to be overridden by subclasses to perform steps necessary for
		/// exiting a state.
		/// </summary>
		/// <param name="context">HA context</param>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException">on failure to enter the state.
		/// 	</exception>
		public abstract void ExitState(HAContext context);

		/// <summary>Move from the existing state to a new state</summary>
		/// <param name="context">HA context</param>
		/// <param name="s">new state</param>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException">on failure to transition to new state.
		/// 	</exception>
		public virtual void SetState(HAContext context, Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.HAState
			 s)
		{
			if (this == s)
			{
				// Aleady in the new state
				return;
			}
			throw new ServiceFailedException("Transtion from state " + this + " to " + s + " is not allowed."
				);
		}

		/// <summary>Check if an operation is supported in a given state.</summary>
		/// <param name="context">HA context</param>
		/// <param name="op">Type of the operation.</param>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException">
		/// if a given type of operation is not
		/// supported in standby state
		/// </exception>
		public abstract void CheckOperation(HAContext context, NameNode.OperationCategory
			 op);

		public abstract bool ShouldPopulateReplQueues();

		/// <returns>String representation of the service state.</returns>
		public override string ToString()
		{
			return state.ToString();
		}
	}
}
