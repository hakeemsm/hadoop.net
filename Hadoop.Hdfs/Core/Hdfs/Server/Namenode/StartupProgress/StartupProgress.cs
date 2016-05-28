using System.Collections.Generic;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// StartupProgress is used in various parts of the namenode codebase to indicate
	/// startup progress.
	/// </summary>
	/// <remarks>
	/// StartupProgress is used in various parts of the namenode codebase to indicate
	/// startup progress.  Its methods provide ways to indicate begin and end of a
	/// <see cref="Phase"/>
	/// or
	/// <see cref="Step"/>
	/// within a phase.  Additional methods provide ways
	/// to associate a step or phase with optional information, such as a file name or
	/// file size.  It also provides counters, which can be incremented by the  caller
	/// to indicate progress through a long-running task.
	/// This class is thread-safe.  Any number of threads may call any methods, even
	/// for the same phase or step, without risk of corrupting internal state.  For
	/// all begin/end methods and set methods, the last one in wins, overwriting any
	/// prior writes.  Instances of
	/// <see cref="Counter"/>
	/// provide an atomic increment
	/// operation to prevent lost updates.
	/// After startup completes, the tracked data is frozen.  Any subsequent updates
	/// or counter increments are no-ops.
	/// For read access, call
	/// <see cref="CreateView()"/>
	/// to create a consistent view with
	/// a clone of the data.
	/// </remarks>
	public class StartupProgress
	{
		internal readonly IDictionary<Phase, PhaseTracking> phases = new ConcurrentHashMap
			<Phase, PhaseTracking>();

		/// <summary>Allows a caller to increment a counter for tracking progress.</summary>
		public interface Counter
		{
			// package-private for access by StartupProgressView
			/// <summary>Atomically increments this counter, adding 1 to the current value.</summary>
			void Increment();
		}

		/// <summary>
		/// Creates a new StartupProgress by initializing internal data structure for
		/// tracking progress of all defined phases.
		/// </summary>
		public StartupProgress()
		{
			foreach (Phase phase in EnumSet.AllOf<Phase>())
			{
				phases[phase] = new PhaseTracking();
			}
		}

		/// <summary>Begins execution of the specified phase.</summary>
		/// <param name="phase">Phase to begin</param>
		public virtual void BeginPhase(Phase phase)
		{
			if (!IsComplete())
			{
				phases[phase].beginTime = Time.MonotonicNow();
			}
		}

		/// <summary>Begins execution of the specified step within the specified phase.</summary>
		/// <param name="phase">Phase to begin</param>
		/// <param name="step">Step to begin</param>
		public virtual void BeginStep(Phase phase, Step step)
		{
			if (!IsComplete())
			{
				LazyInitStep(phase, step).beginTime = Time.MonotonicNow();
			}
		}

		/// <summary>Ends execution of the specified phase.</summary>
		/// <param name="phase">Phase to end</param>
		public virtual void EndPhase(Phase phase)
		{
			if (!IsComplete())
			{
				phases[phase].endTime = Time.MonotonicNow();
			}
		}

		/// <summary>Ends execution of the specified step within the specified phase.</summary>
		/// <param name="phase">Phase to end</param>
		/// <param name="step">Step to end</param>
		public virtual void EndStep(Phase phase, Step step)
		{
			if (!IsComplete())
			{
				LazyInitStep(phase, step).endTime = Time.MonotonicNow();
			}
		}

		/// <summary>Returns the current run status of the specified phase.</summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>Status run status of phase</returns>
		public virtual Status GetStatus(Phase phase)
		{
			PhaseTracking tracking = phases[phase];
			if (tracking.beginTime == long.MinValue)
			{
				return Status.Pending;
			}
			else
			{
				if (tracking.endTime == long.MinValue)
				{
					return Status.Running;
				}
				else
				{
					return Status.Complete;
				}
			}
		}

		/// <summary>Returns a counter associated with the specified phase and step.</summary>
		/// <remarks>
		/// Returns a counter associated with the specified phase and step.  Typical
		/// usage is to increment a counter within a tight loop.  Callers may use this
		/// method to obtain a counter once and then increment that instance repeatedly
		/// within a loop.  This prevents redundant lookup operations and object
		/// creation within the tight loop.  Incrementing the counter is an atomic
		/// operation, so there is no risk of lost updates even if multiple threads
		/// increment the same counter.
		/// </remarks>
		/// <param name="phase">Phase to get</param>
		/// <param name="step">Step to get</param>
		/// <returns>Counter associated with phase and step</returns>
		public virtual StartupProgress.Counter GetCounter(Phase phase, Step step)
		{
			if (!IsComplete())
			{
				StepTracking tracking = LazyInitStep(phase, step);
				return new _Counter_154(tracking);
			}
			else
			{
				return new _Counter_161();
			}
		}

		private sealed class _Counter_154 : StartupProgress.Counter
		{
			public _Counter_154(StepTracking tracking)
			{
				this.tracking = tracking;
			}

			public void Increment()
			{
				tracking.count.IncrementAndGet();
			}

			private readonly StepTracking tracking;
		}

		private sealed class _Counter_161 : StartupProgress.Counter
		{
			public _Counter_161()
			{
			}

			public void Increment()
			{
			}
		}

		// no-op, because startup has completed
		/// <summary>Sets counter to the specified value.</summary>
		/// <param name="phase">Phase to set</param>
		/// <param name="step">Step to set</param>
		/// <param name="count">long to set</param>
		public virtual void SetCount(Phase phase, Step step, long count)
		{
			LazyInitStep(phase, step).count.Set(count);
		}

		/// <summary>Sets the optional file name associated with the specified phase.</summary>
		/// <remarks>
		/// Sets the optional file name associated with the specified phase.  For
		/// example, this can be used while loading fsimage to indicate the full path to
		/// the fsimage file.
		/// </remarks>
		/// <param name="phase">Phase to set</param>
		/// <param name="file">String file name to set</param>
		public virtual void SetFile(Phase phase, string file)
		{
			if (!IsComplete())
			{
				phases[phase].file = file;
			}
		}

		/// <summary>Sets the optional size in bytes associated with the specified phase.</summary>
		/// <remarks>
		/// Sets the optional size in bytes associated with the specified phase.  For
		/// example, this can be used while loading fsimage to indicate the size of the
		/// fsimage file.
		/// </remarks>
		/// <param name="phase">Phase to set</param>
		/// <param name="size">long to set</param>
		public virtual void SetSize(Phase phase, long size)
		{
			if (!IsComplete())
			{
				phases[phase].size = size;
			}
		}

		/// <summary>Sets the total associated with the specified phase and step.</summary>
		/// <remarks>
		/// Sets the total associated with the specified phase and step.  For example,
		/// this can be used while loading edits to indicate the number of operations to
		/// be applied.
		/// </remarks>
		/// <param name="phase">Phase to set</param>
		/// <param name="step">Step to set</param>
		/// <param name="total">long to set</param>
		public virtual void SetTotal(Phase phase, Step step, long total)
		{
			if (!IsComplete())
			{
				LazyInitStep(phase, step).total = total;
			}
		}

		/// <summary>
		/// Creates a
		/// <see cref="StartupProgressView"/>
		/// containing data cloned from this
		/// StartupProgress.  Subsequent updates to this StartupProgress will not be
		/// shown in the view.  This gives a consistent, unchanging view for callers
		/// that need to perform multiple related read operations.  Calculations that
		/// require aggregation, such as overall percent complete, will not be impacted
		/// by mutations performed in other threads mid-way through the calculation.
		/// </summary>
		/// <returns>StartupProgressView containing cloned data</returns>
		public virtual StartupProgressView CreateView()
		{
			return new StartupProgressView(this);
		}

		/// <summary>
		/// Returns true if the entire startup process has completed, determined by
		/// checking if each phase is complete.
		/// </summary>
		/// <returns>boolean true if the entire startup process has completed</returns>
		private bool IsComplete()
		{
			foreach (Phase phase in EnumSet.AllOf<Phase>())
			{
				if (GetStatus(phase) != Status.Complete)
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>
		/// Lazily initializes the internal data structure for tracking the specified
		/// phase and step.
		/// </summary>
		/// <remarks>
		/// Lazily initializes the internal data structure for tracking the specified
		/// phase and step.  Returns either the newly initialized data structure or the
		/// existing one.  Initialization is atomic, so there is no risk of lost updates
		/// even if multiple threads attempt to initialize the same step simultaneously.
		/// </remarks>
		/// <param name="phase">Phase to initialize</param>
		/// <param name="step">Step to initialize</param>
		/// <returns>StepTracking newly initialized, or existing if found</returns>
		private StepTracking LazyInitStep(Phase phase, Step step)
		{
			ConcurrentMap<Step, StepTracking> steps = phases[phase].steps;
			if (!steps.Contains(step))
			{
				steps.PutIfAbsent(step, new StepTracking());
			}
			return steps[step];
		}
	}
}
