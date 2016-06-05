using System.Collections;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	/// <summary>State machine topology.</summary>
	/// <remarks>
	/// State machine topology.
	/// This object is semantically immutable.  If you have a
	/// StateMachineFactory there's no operation in the API that changes
	/// its semantic properties.
	/// </remarks>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public sealed class StateMachineFactory<Operand, State, Eventtype, Event>
		where State : Enum<STATE>
		where Eventtype : Enum<EVENTTYPE>
	{
		private readonly StateMachineFactory.TransitionsListNode transitionsListNode;

		private IDictionary<STATE, IDictionary<EVENTTYPE, StateMachineFactory.Transition<
			OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

		private STATE defaultInitialState;

		private readonly bool optimized;

		/// <summary>
		/// Constructor
		/// This is the only constructor in the API.
		/// </summary>
		public StateMachineFactory(STATE defaultInitialState)
		{
			this.transitionsListNode = null;
			this.defaultInitialState = defaultInitialState;
			this.optimized = false;
			this.stateMachineTable = null;
		}

		private StateMachineFactory(Org.Apache.Hadoop.Yarn.State.StateMachineFactory<OPERAND
			, STATE, EVENTTYPE, EVENT> that, StateMachineFactory.ApplicableTransition<OPERAND
			, STATE, EVENTTYPE, EVENT> t)
		{
			this.defaultInitialState = that.defaultInitialState;
			this.transitionsListNode = new StateMachineFactory.TransitionsListNode(this, t, that
				.transitionsListNode);
			this.optimized = false;
			this.stateMachineTable = null;
		}

		private StateMachineFactory(Org.Apache.Hadoop.Yarn.State.StateMachineFactory<OPERAND
			, STATE, EVENTTYPE, EVENT> that, bool optimized)
		{
			this.defaultInitialState = that.defaultInitialState;
			this.transitionsListNode = that.transitionsListNode;
			this.optimized = optimized;
			if (optimized)
			{
				MakeStateMachineTable();
			}
			else
			{
				stateMachineTable = null;
			}
		}

		private interface ApplicableTransition<Operand, State, Eventtype, Event>
			where State : Enum<STATE>
			where Eventtype : Enum<EVENTTYPE>
		{
			void Apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
		}

		private class TransitionsListNode
		{
			internal readonly StateMachineFactory.ApplicableTransition<OPERAND, STATE, EVENTTYPE
				, EVENT> transition;

			internal readonly StateMachineFactory.TransitionsListNode next;

			internal TransitionsListNode(StateMachineFactory<Operand, State, Eventtype, Event
				> _enclosing, StateMachineFactory.ApplicableTransition<OPERAND, STATE, EVENTTYPE
				, EVENT> transition, StateMachineFactory.TransitionsListNode next)
			{
				this._enclosing = _enclosing;
				this.transition = transition;
				this.next = next;
			}

			private readonly StateMachineFactory<Operand, State, Eventtype, Event> _enclosing;
		}

		private class ApplicableSingleOrMultipleTransition<Operand, State, Eventtype, Event
			> : StateMachineFactory.ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>
			where State : Enum<STATE>
			where Eventtype : Enum<EVENTTYPE>
		{
			internal readonly STATE preState;

			internal readonly EVENTTYPE eventType;

			internal readonly StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, EVENT
				> transition;

			internal ApplicableSingleOrMultipleTransition(STATE preState, EVENTTYPE eventType
				, StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition)
			{
				this.preState = preState;
				this.eventType = eventType;
				this.transition = transition;
			}

			public virtual void Apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject
				)
			{
				IDictionary<EVENTTYPE, StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, 
					EVENT>> transitionMap = subject.stateMachineTable[preState];
				if (transitionMap == null)
				{
					// I use HashMap here because I would expect most EVENTTYPE's to not
					//  apply out of a particular state, so FSM sizes would be 
					//  quadratic if I use EnumMap's here as I do at the top level.
					transitionMap = new Dictionary<EVENTTYPE, StateMachineFactory.Transition<OPERAND, 
						STATE, EVENTTYPE, EVENT>>();
					subject.stateMachineTable[preState] = transitionMap;
				}
				transitionMap[eventType] = transition;
			}
		}

		/// <returns>
		/// a NEW StateMachineFactory just like
		/// <c>this</c>
		/// with the current
		/// transition added as a new legal transition.  This overload
		/// has no hook object.
		/// Note that the returned StateMachineFactory is a distinct
		/// object.
		/// This method is part of the API.
		/// </returns>
		/// <param name="preState">pre-transition state</param>
		/// <param name="postState">post-transition state</param>
		/// <param name="eventType">stimulus for the transition</param>
		public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> AddTransition(STATE 
			preState, STATE postState, EVENTTYPE eventType)
		{
			return AddTransition(preState, postState, eventType, null);
		}

		/// <returns>
		/// a NEW StateMachineFactory just like
		/// <c>this</c>
		/// with the current
		/// transition added as a new legal transition.  This overload
		/// has no hook object.
		/// Note that the returned StateMachineFactory is a distinct
		/// object.
		/// This method is part of the API.
		/// </returns>
		/// <param name="preState">pre-transition state</param>
		/// <param name="postState">post-transition state</param>
		/// <param name="eventTypes">List of stimuli for the transitions</param>
		public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> AddTransition(STATE 
			preState, STATE postState, ICollection<EVENTTYPE> eventTypes)
		{
			return AddTransition(preState, postState, eventTypes, null);
		}

		/// <returns>
		/// a NEW StateMachineFactory just like
		/// <c>this</c>
		/// with the current
		/// transition added as a new legal transition
		/// Note that the returned StateMachineFactory is a distinct
		/// object.
		/// This method is part of the API.
		/// </returns>
		/// <param name="preState">pre-transition state</param>
		/// <param name="postState">post-transition state</param>
		/// <param name="eventTypes">List of stimuli for the transitions</param>
		/// <param name="hook">transition hook</param>
		public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> AddTransition(STATE 
			preState, STATE postState, ICollection<EVENTTYPE> eventTypes, SingleArcTransition
			<OPERAND, EVENT> hook)
		{
			StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;
			foreach (EVENTTYPE @event in eventTypes)
			{
				if (factory == null)
				{
					factory = AddTransition(preState, postState, @event, hook);
				}
				else
				{
					factory = factory.AddTransition(preState, postState, @event, hook);
				}
			}
			return factory;
		}

		/// <returns>
		/// a NEW StateMachineFactory just like
		/// <c>this</c>
		/// with the current
		/// transition added as a new legal transition
		/// Note that the returned StateMachineFactory is a distinct object.
		/// This method is part of the API.
		/// </returns>
		/// <param name="preState">pre-transition state</param>
		/// <param name="postState">post-transition state</param>
		/// <param name="eventType">stimulus for the transition</param>
		/// <param name="hook">transition hook</param>
		public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> AddTransition(STATE 
			preState, STATE postState, EVENTTYPE eventType, SingleArcTransition<OPERAND, EVENT
			> hook)
		{
			return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(this, new StateMachineFactory.ApplicableSingleOrMultipleTransition
				<OPERAND, STATE, EVENTTYPE, EVENT>(preState, eventType, new StateMachineFactory.SingleInternalArc
				(this, postState, hook)));
		}

		/// <returns>
		/// a NEW StateMachineFactory just like
		/// <c>this</c>
		/// with the current
		/// transition added as a new legal transition
		/// Note that the returned StateMachineFactory is a distinct object.
		/// This method is part of the API.
		/// </returns>
		/// <param name="preState">pre-transition state</param>
		/// <param name="postStates">valid post-transition states</param>
		/// <param name="eventType">stimulus for the transition</param>
		/// <param name="hook">transition hook</param>
		public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> AddTransition(STATE 
			preState, ICollection<STATE> postStates, EVENTTYPE eventType, MultipleArcTransition
			<OPERAND, EVENT, STATE> hook)
		{
			return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(this, new StateMachineFactory.ApplicableSingleOrMultipleTransition
				<OPERAND, STATE, EVENTTYPE, EVENT>(preState, eventType, new StateMachineFactory.MultipleInternalArc
				(this, postStates, hook)));
		}

		/// <returns>
		/// a StateMachineFactory just like
		/// <c>this</c>
		/// , except that if
		/// you won't need any synchronization to build a state machine
		/// Note that the returned StateMachineFactory is a distinct object.
		/// This method is part of the API.
		/// The only way you could distinguish the returned
		/// StateMachineFactory from
		/// <c>this</c>
		/// would be by
		/// measuring the performance of the derived
		/// <c>StateMachine</c>
		/// you can get from it.
		/// Calling this is optional.  It doesn't change the semantics of the factory,
		/// if you call it then when you use the factory there is no synchronization.
		/// </returns>
		public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> InstallTopology()
		{
			return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(this, true);
		}

		/// <summary>Effect a transition due to the effecting stimulus.</summary>
		/// <param name="state">current state</param>
		/// <param name="eventType">trigger to initiate the transition</param>
		/// <param name="cause">causal eventType context</param>
		/// <returns>transitioned state</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.State.InvalidStateTransitonException"/>
		private STATE DoTransition(OPERAND operand, STATE oldState, EVENTTYPE eventType, 
			EVENT @event)
		{
			// We can assume that stateMachineTable is non-null because we call
			//  maybeMakeStateMachineTable() when we build an InnerStateMachine ,
			//  and this code only gets called from inside a working InnerStateMachine .
			IDictionary<EVENTTYPE, StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, 
				EVENT>> transitionMap = stateMachineTable[oldState];
			if (transitionMap != null)
			{
				StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = transitionMap
					[eventType];
				if (transition != null)
				{
					return transition.DoTransition(operand, oldState, @event, eventType);
				}
			}
			throw new InvalidStateTransitonException(oldState, eventType);
		}

		private void MaybeMakeStateMachineTable()
		{
			lock (this)
			{
				if (stateMachineTable == null)
				{
					MakeStateMachineTable();
				}
			}
		}

		private void MakeStateMachineTable()
		{
			Stack<StateMachineFactory.ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>>
				 stack = new Stack<StateMachineFactory.ApplicableTransition<OPERAND, STATE, EVENTTYPE
				, EVENT>>();
			IDictionary<STATE, IDictionary<EVENTTYPE, StateMachineFactory.Transition<OPERAND, 
				STATE, EVENTTYPE, EVENT>>> prototype = new Dictionary<STATE, IDictionary<EVENTTYPE
				, StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();
			prototype[defaultInitialState] = null;
			// I use EnumMap here because it'll be faster and denser.  I would
			//  expect most of the states to have at least one transition.
			stateMachineTable = new EnumMap<STATE, IDictionary<EVENTTYPE, StateMachineFactory.Transition
				<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);
			for (StateMachineFactory.TransitionsListNode cursor = transitionsListNode; cursor
				 != null; cursor = cursor.next)
			{
				stack.Push(cursor.transition);
			}
			while (!stack.IsEmpty())
			{
				stack.Pop().Apply(this);
			}
		}

		private interface Transition<Operand, State, Eventtype, Event>
			where State : Enum<STATE>
			where Eventtype : Enum<EVENTTYPE>
		{
			STATE DoTransition(OPERAND operand, STATE oldState, EVENT @event, EVENTTYPE eventType
				);
		}

		private class SingleInternalArc : StateMachineFactory.Transition<OPERAND, STATE, 
			EVENTTYPE, EVENT>
		{
			private STATE postState;

			private SingleArcTransition<OPERAND, EVENT> hook;

			internal SingleInternalArc(StateMachineFactory<Operand, State, Eventtype, Event> 
				_enclosing, STATE postState, SingleArcTransition<OPERAND, EVENT> hook)
			{
				this._enclosing = _enclosing;
				// transition hook
				this.postState = postState;
				this.hook = hook;
			}

			public virtual STATE DoTransition(OPERAND operand, STATE oldState, EVENT @event, 
				EVENTTYPE eventType)
			{
				if (this.hook != null)
				{
					this.hook.Transition(operand, @event);
				}
				return this.postState;
			}

			private readonly StateMachineFactory<Operand, State, Eventtype, Event> _enclosing;
		}

		private class MultipleInternalArc : StateMachineFactory.Transition<OPERAND, STATE
			, EVENTTYPE, EVENT>
		{
			private ICollection<STATE> validPostStates;

			private MultipleArcTransition<OPERAND, EVENT, STATE> hook;

			internal MultipleInternalArc(StateMachineFactory<Operand, State, Eventtype, Event
				> _enclosing, ICollection<STATE> postStates, MultipleArcTransition<OPERAND, EVENT
				, STATE> hook)
			{
				this._enclosing = _enclosing;
				// Fields
				// transition hook
				this.validPostStates = postStates;
				this.hook = hook;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.State.InvalidStateTransitonException"/>
			public virtual STATE DoTransition(OPERAND operand, STATE oldState, EVENT @event, 
				EVENTTYPE eventType)
			{
				STATE postState = this.hook.Transition(operand, @event);
				if (!this.validPostStates.Contains(postState))
				{
					throw new InvalidStateTransitonException(oldState, eventType);
				}
				return postState;
			}

			private readonly StateMachineFactory<Operand, State, Eventtype, Event> _enclosing;
		}

		/*
		* @return a {@link StateMachine} that starts in
		*         {@code initialState} and whose {@link Transition} s are
		*         applied to {@code operand} .
		*
		*         This is part of the API.
		*
		* @param operand the object upon which the returned
		*                {@link StateMachine} will operate.
		* @param initialState the state in which the returned
		*                {@link StateMachine} will start.
		*
		*/
		public StateMachine<STATE, EVENTTYPE, EVENT> Make(OPERAND operand, STATE initialState
			)
		{
			return new StateMachineFactory.InternalStateMachine(this, operand, initialState);
		}

		/*
		* @return a {@link StateMachine} that starts in the default initial
		*          state and whose {@link Transition} s are applied to
		*          {@code operand} .
		*
		*         This is part of the API.
		*
		* @param operand the object upon which the returned
		*                {@link StateMachine} will operate.
		*
		*/
		public StateMachine<STATE, EVENTTYPE, EVENT> Make(OPERAND operand)
		{
			return new StateMachineFactory.InternalStateMachine(this, operand, defaultInitialState
				);
		}

		private class InternalStateMachine : StateMachine<STATE, EVENTTYPE, EVENT>
		{
			private readonly OPERAND operand;

			private STATE currentState;

			internal InternalStateMachine(StateMachineFactory<Operand, State, Eventtype, Event
				> _enclosing, OPERAND operand, STATE initialState)
			{
				this._enclosing = _enclosing;
				this.operand = operand;
				this.currentState = initialState;
				if (!this._enclosing.optimized)
				{
					this._enclosing.MaybeMakeStateMachineTable();
				}
			}

			public virtual STATE GetCurrentState()
			{
				lock (this)
				{
					return this.currentState;
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.State.InvalidStateTransitonException"/>
			public virtual STATE DoTransition(EVENTTYPE eventType, EVENT @event)
			{
				lock (this)
				{
					this.currentState = this._enclosing._enclosing.DoTransition(this.operand, this.currentState
						, eventType, @event);
					return this.currentState;
				}
			}

			private readonly StateMachineFactory<Operand, State, Eventtype, Event> _enclosing;
		}

		/// <summary>Generate a graph represents the state graph of this StateMachine</summary>
		/// <param name="name">graph name</param>
		/// <returns>Graph object generated</returns>
		public Graph GenerateStateGraph(string name)
		{
			MaybeMakeStateMachineTable();
			Graph g = new Graph(name);
			foreach (STATE startState in stateMachineTable.Keys)
			{
				IDictionary<EVENTTYPE, StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, 
					EVENT>> transitions = stateMachineTable[startState];
				foreach (KeyValuePair<EVENTTYPE, StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE
					, EVENT>> entry in transitions)
				{
					StateMachineFactory.Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = entry
						.Value;
					if (transition is StateMachineFactory.SingleInternalArc)
					{
						StateMachineFactory.SingleInternalArc sa = (StateMachineFactory.SingleInternalArc
							)transition;
						Graph.Node fromNode = g.GetNode(startState.ToString());
						Graph.Node toNode = g.GetNode(sa.postState.ToString());
						fromNode.AddEdge(toNode, entry.Key.ToString());
					}
					else
					{
						if (transition is StateMachineFactory.MultipleInternalArc)
						{
							StateMachineFactory.MultipleInternalArc ma = (StateMachineFactory.MultipleInternalArc
								)transition;
							IEnumerator iter = ma.validPostStates.GetEnumerator();
							while (iter.HasNext())
							{
								Graph.Node fromNode = g.GetNode(startState.ToString());
								Graph.Node toNode = g.GetNode(iter.Next().ToString());
								fromNode.AddEdge(toNode, entry.Key.ToString());
							}
						}
					}
				}
			}
			return g;
		}
	}
}
