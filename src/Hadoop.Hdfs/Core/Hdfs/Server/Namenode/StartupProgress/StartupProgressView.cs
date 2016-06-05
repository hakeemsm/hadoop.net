using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// StartupProgressView is an immutable, consistent, read-only view of namenode
	/// startup progress.
	/// </summary>
	/// <remarks>
	/// StartupProgressView is an immutable, consistent, read-only view of namenode
	/// startup progress.  Callers obtain an instance by calling
	/// <see cref="StartupProgress.CreateView()"/>
	/// to clone current startup progress state.
	/// Subsequent updates to startup progress will not alter the view.  This isolates
	/// the reader from ongoing updates and establishes a guarantee that the values
	/// returned by the view are consistent and unchanging across multiple related
	/// read operations.  Calculations that require aggregation, such as overall
	/// percent complete, will not be impacted by mutations performed in other threads
	/// mid-way through the calculation.
	/// Methods that return primitive long may return
	/// <see cref="long.MinValue"/>
	/// as a
	/// sentinel value to indicate that the property is undefined.
	/// </remarks>
	public class StartupProgressView
	{
		private readonly IDictionary<Phase, PhaseTracking> phases;

		/// <summary>Returns the sum of the counter values for all steps in the specified phase.
		/// 	</summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>long sum of counter values for all steps</returns>
		public virtual long GetCount(Phase phase)
		{
			long sum = 0;
			foreach (Step step in GetSteps(phase))
			{
				sum += GetCount(phase, step);
			}
			return sum;
		}

		/// <summary>Returns the counter value for the specified phase and step.</summary>
		/// <param name="phase">Phase to get</param>
		/// <param name="step">Step to get</param>
		/// <returns>long counter value for phase and step</returns>
		public virtual long GetCount(Phase phase, Step step)
		{
			StepTracking tracking = GetStepTracking(phase, step);
			return tracking != null ? tracking.count.Get() : 0;
		}

		/// <summary>
		/// Returns overall elapsed time, calculated as time between start of loading
		/// fsimage and end of safemode.
		/// </summary>
		/// <returns>long elapsed time</returns>
		public virtual long GetElapsedTime()
		{
			return GetElapsedTime(phases[Phase.LoadingFsimage], phases[Phase.Safemode]);
		}

		/// <summary>
		/// Returns elapsed time for the specified phase, calculated as (end - begin) if
		/// phase is complete or (now - begin) if phase is running or 0 if the phase is
		/// still pending.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>long elapsed time</returns>
		public virtual long GetElapsedTime(Phase phase)
		{
			return GetElapsedTime(phases[phase]);
		}

		/// <summary>
		/// Returns elapsed time for the specified phase and step, calculated as
		/// (end - begin) if step is complete or (now - begin) if step is running or 0
		/// if the step is still pending.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <param name="step">Step to get</param>
		/// <returns>long elapsed time</returns>
		public virtual long GetElapsedTime(Phase phase, Step step)
		{
			return GetElapsedTime(GetStepTracking(phase, step));
		}

		/// <summary>
		/// Returns the optional file name associated with the specified phase, possibly
		/// null.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>String optional file name, possibly null</returns>
		public virtual string GetFile(Phase phase)
		{
			return phases[phase].file;
		}

		/// <summary>
		/// Returns overall percent complete, calculated by aggregating percent complete
		/// of all phases.
		/// </summary>
		/// <remarks>
		/// Returns overall percent complete, calculated by aggregating percent complete
		/// of all phases.  This is an approximation that assumes all phases have equal
		/// running time.  In practice, this isn't true, but there isn't sufficient
		/// information available to predict proportional weights for each phase.
		/// </remarks>
		/// <returns>float percent complete</returns>
		public virtual float GetPercentComplete()
		{
			if (GetStatus(Phase.Safemode) == Status.Complete)
			{
				return 1.0f;
			}
			else
			{
				float total = 0.0f;
				int numPhases = 0;
				foreach (Phase phase in phases.Keys)
				{
					++numPhases;
					total += GetPercentComplete(phase);
				}
				return GetBoundedPercent(total / numPhases);
			}
		}

		/// <summary>
		/// Returns percent complete for the specified phase, calculated by aggregating
		/// the counter values and totals for all steps within the phase.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>float percent complete</returns>
		public virtual float GetPercentComplete(Phase phase)
		{
			if (GetStatus(phase) == Status.Complete)
			{
				return 1.0f;
			}
			else
			{
				long total = GetTotal(phase);
				long count = 0;
				foreach (Step step in GetSteps(phase))
				{
					count += GetCount(phase, step);
				}
				return total > 0 ? GetBoundedPercent(1.0f * count / total) : 0.0f;
			}
		}

		/// <summary>
		/// Returns percent complete for the specified phase and step, calculated as
		/// counter value divided by total.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <param name="step">Step to get</param>
		/// <returns>float percent complete</returns>
		public virtual float GetPercentComplete(Phase phase, Step step)
		{
			if (GetStatus(phase) == Status.Complete)
			{
				return 1.0f;
			}
			else
			{
				long total = GetTotal(phase, step);
				long count = GetCount(phase, step);
				return total > 0 ? GetBoundedPercent(1.0f * count / total) : 0.0f;
			}
		}

		/// <summary>Returns all phases.</summary>
		/// <returns>Iterable<Phase> containing all phases</returns>
		public virtual IEnumerable<Phase> GetPhases()
		{
			return EnumSet.AllOf<Phase>();
		}

		/// <summary>Returns all steps within a phase.</summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>Iterable<Step> all steps</returns>
		public virtual IEnumerable<Step> GetSteps(Phase phase)
		{
			return new TreeSet<Step>(phases[phase].steps.Keys);
		}

		/// <summary>
		/// Returns the optional size in bytes associated with the specified phase,
		/// possibly Long.MIN_VALUE if undefined.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>long optional size in bytes, possibly Long.MIN_VALUE</returns>
		public virtual long GetSize(Phase phase)
		{
			return phases[phase].size;
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

		/// <summary>Returns the sum of the totals for all steps in the specified phase.</summary>
		/// <param name="phase">Phase to get</param>
		/// <returns>long sum of totals for all steps</returns>
		public virtual long GetTotal(Phase phase)
		{
			long sum = 0;
			foreach (StepTracking tracking in phases[phase].steps.Values)
			{
				if (tracking.total != long.MinValue)
				{
					sum += tracking.total;
				}
			}
			return sum;
		}

		/// <summary>Returns the total for the specified phase and step.</summary>
		/// <param name="phase">Phase to get</param>
		/// <param name="step">Step to get</param>
		/// <returns>long total</returns>
		public virtual long GetTotal(Phase phase, Step step)
		{
			StepTracking tracking = GetStepTracking(phase, step);
			return tracking != null && tracking.total != long.MinValue ? tracking.total : 0;
		}

		/// <summary>
		/// Creates a new StartupProgressView by cloning data from the specified
		/// StartupProgress.
		/// </summary>
		/// <param name="prog">StartupProgress to clone</param>
		internal StartupProgressView(StartupProgress prog)
		{
			phases = new Dictionary<Phase, PhaseTracking>();
			foreach (KeyValuePair<Phase, PhaseTracking> entry in prog.phases)
			{
				phases[entry.Key] = entry.Value.Clone();
			}
		}

		/// <summary>
		/// Returns elapsed time, calculated as (end - begin) if both are defined or
		/// (now - begin) if end is undefined or 0 if both are undefined.
		/// </summary>
		/// <remarks>
		/// Returns elapsed time, calculated as (end - begin) if both are defined or
		/// (now - begin) if end is undefined or 0 if both are undefined.  Begin and end
		/// time come from the same AbstractTracking instance.
		/// </remarks>
		/// <param name="tracking">AbstractTracking containing begin and end time</param>
		/// <returns>long elapsed time</returns>
		private long GetElapsedTime(AbstractTracking tracking)
		{
			return GetElapsedTime(tracking, tracking);
		}

		/// <summary>
		/// Returns elapsed time, calculated as (end - begin) if both are defined or
		/// (now - begin) if end is undefined or 0 if both are undefined.
		/// </summary>
		/// <remarks>
		/// Returns elapsed time, calculated as (end - begin) if both are defined or
		/// (now - begin) if end is undefined or 0 if both are undefined.  Begin and end
		/// time may come from different AbstractTracking instances.
		/// </remarks>
		/// <param name="beginTracking">AbstractTracking containing begin time</param>
		/// <param name="endTracking">AbstractTracking containing end time</param>
		/// <returns>long elapsed time</returns>
		private long GetElapsedTime(AbstractTracking beginTracking, AbstractTracking endTracking
			)
		{
			long elapsed;
			if (beginTracking != null && beginTracking.beginTime != long.MinValue && endTracking
				 != null && endTracking.endTime != long.MinValue)
			{
				elapsed = endTracking.endTime - beginTracking.beginTime;
			}
			else
			{
				if (beginTracking != null && beginTracking.beginTime != long.MinValue)
				{
					elapsed = Time.MonotonicNow() - beginTracking.beginTime;
				}
				else
				{
					elapsed = 0;
				}
			}
			return Math.Max(0, elapsed);
		}

		/// <summary>
		/// Returns the StepTracking internal data structure for the specified phase
		/// and step, possibly null if not found.
		/// </summary>
		/// <param name="phase">Phase to get</param>
		/// <param name="step">Step to get</param>
		/// <returns>StepTracking for phase and step, possibly null</returns>
		private StepTracking GetStepTracking(Phase phase, Step step)
		{
			PhaseTracking phaseTracking = phases[phase];
			IDictionary<Step, StepTracking> steps = phaseTracking != null ? phaseTracking.steps
				 : null;
			return steps != null ? steps[step] : null;
		}

		/// <summary>Returns the given value restricted to the range [0.0, 1.0].</summary>
		/// <param name="percent">float value to restrict</param>
		/// <returns>float value restricted to range [0.0, 1.0]</returns>
		private static float GetBoundedPercent(float percent)
		{
			return Math.Max(0.0f, Math.Min(1.0f, percent));
		}
	}
}
