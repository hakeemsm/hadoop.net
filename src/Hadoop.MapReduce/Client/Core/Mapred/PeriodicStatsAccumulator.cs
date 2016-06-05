using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This abstract class that represents a bucketed series of
	/// measurements of a quantity being measured in a running task
	/// attempt.
	/// </summary>
	/// <remarks>
	/// This abstract class that represents a bucketed series of
	/// measurements of a quantity being measured in a running task
	/// attempt.
	/// <p>The sole constructor is called with a count, which is the
	/// number of buckets into which we evenly divide the spectrum of
	/// progress from 0.0D to 1.0D .  In the future we may provide for
	/// custom split points that don't have to be uniform.
	/// <p>A subclass determines how we fold readings for portions of a
	/// bucket and how we interpret the readings by overriding
	/// <c>extendInternal(...)</c>
	/// and
	/// <c>initializeInterval()</c>
	/// </remarks>
	public abstract class PeriodicStatsAccumulator
	{
		protected internal readonly int count;

		protected internal readonly int[] values;

		internal class StatsetState
		{
			internal int oldValue = 0;

			internal double oldProgress = 0.0D;

			internal double currentAccumulation = 0.0D;
			// The range of progress from 0.0D through 1.0D is divided into
			//  count "progress segments".  This object accumulates an
			//  estimate of the effective value of a time-varying value during
			//  the zero-based i'th progress segment, ranging from i/count
			//  through (i+1)/count . 
			// This is an abstract class.  We have two implementations: one
			//  for monotonically increasing time-dependent variables
			//  [currently, CPU time in milliseconds and wallclock time in
			//  milliseconds] and one for quantities that can vary arbitrarily
			//  over time, currently virtual and physical memory used, in
			//  kilobytes. 
			// We carry int's here.  This saves a lot of JVM heap space in the
			//  job tracker per running task attempt [200 bytes per] but it
			//  has a small downside.
			// No task attempt can run for more than 57 days nor occupy more
			//  than two terabytes of virtual memory. 
		}

		internal PeriodicStatsAccumulator.StatsetState state = new PeriodicStatsAccumulator.StatsetState
			();

		internal PeriodicStatsAccumulator(int count)
		{
			// We provide this level of indirection to reduce the memory
			//  footprint of done task attempts.  When a task's progress
			//  reaches 1.0D, we delete this objecte StatsetState.
			this.count = count;
			this.values = new int[count];
			for (int i = 0; i < count; ++i)
			{
				values[i] = -1;
			}
		}

		protected internal virtual int[] GetValues()
		{
			return values;
		}

		// The concrete implementation of this abstract function
		//  accumulates more data into the current progress segment.
		//  newProgress [from the call] and oldProgress [from the object]
		//  must be in [or at the border of] a single progress segment.
		/// <summary>adds a new reading to the current bucket.</summary>
		/// <param name="newProgress">
		/// the endpoint of the interval this new
		/// reading covers
		/// </param>
		/// <param name="newValue">
		/// the value of the reading at
		/// <paramref name="newProgress"/>
		/// 
		/// The class has three instance variables,
		/// <c>oldProgress</c>
		/// and
		/// <c>oldValue</c>
		/// and
		/// <c>currentAccumulation</c>
		/// .
		/// <c>extendInternal</c>
		/// can count on three things:
		/// 1: The first time it's called in a particular instance, both
		/// oldXXX's will be zero.
		/// 2: oldXXX for a later call is the value of newXXX of the
		/// previous call.  This ensures continuity in accumulation from
		/// one call to the next.
		/// 3:
		/// <c>currentAccumulation</c>
		/// is owned by
		/// <c>initializeInterval</c>
		/// and
		/// <c>extendInternal</c>
		/// .
		/// </param>
		protected internal abstract void ExtendInternal(double newProgress, int newValue);

		// What has to be done when you open a new interval
		/// <summary>initializes the state variables to be ready for a new interval</summary>
		protected internal virtual void InitializeInterval()
		{
			state.currentAccumulation = 0.0D;
		}

		// called for each new reading
		/// <summary>
		/// This method calls
		/// <c>extendInternal</c>
		/// at least once.  It
		/// divides the current progress interval [from the last call's
		/// <paramref name="newProgress"/>
		/// to this call's
		/// <paramref name="newProgress"/>
		/// ]
		/// into one or more subintervals by splitting at any point which
		/// is an interval boundary if there are any such points.  It
		/// then calls
		/// <c>extendInternal</c>
		/// for each subinterval, or the
		/// whole interval if there are no splitting points.
		/// <p>For example, if the value was
		/// <c>300</c>
		/// last time with
		/// <c>0.3</c>
		/// progress, and count is
		/// <c>5</c>
		/// , and you get a
		/// new reading with the variable at
		/// <c>700</c>
		/// and progress at
		/// <c>0.7</c>
		/// , you get three calls to
		/// <c>extendInternal</c>
		/// :
		/// one extending from progress
		/// <c>0.3</c>
		/// to
		/// <c>0.4</c>
		/// [the
		/// next boundary] with a value of
		/// <c>400</c>
		/// , the next one
		/// through
		/// <c>0.6</c>
		/// with a  value of
		/// <c>600</c>
		/// , and finally
		/// one at
		/// <c>700</c>
		/// with a progress of
		/// <c>0.7</c>
		/// .
		/// </summary>
		/// <param name="newProgress">
		/// the endpoint of the progress range this new
		/// reading covers
		/// </param>
		/// <param name="newValue">
		/// the value of the reading at
		/// <paramref name="newProgress"/>
		/// 
		/// </param>
		protected internal virtual void Extend(double newProgress, int newValue)
		{
			if (state == null || newProgress < state.oldProgress)
			{
				return;
			}
			// This correctness of this code depends on 100% * count = count.
			int oldIndex = (int)(state.oldProgress * count);
			int newIndex = (int)(newProgress * count);
			int originalOldValue = state.oldValue;
			double fullValueDistance = (double)newValue - state.oldValue;
			double fullProgressDistance = newProgress - state.oldProgress;
			double originalOldProgress = state.oldProgress;
			// In this loop we detect each subinterval boundary within the
			//  range from the old progress to the new one.  Then we
			//  interpolate the value from the old value to the new one to
			//  infer what its value might have been at each such boundary.
			//  Lastly we make the necessary calls to extendInternal to fold
			//  in the data for each trapazoid where no such trapazoid
			//  crosses a boundary.
			for (int closee = oldIndex; closee < newIndex; ++closee)
			{
				double interpolationProgress = (double)(closee + 1) / count;
				// In floats, x * y / y might not equal y.
				interpolationProgress = Math.Min(interpolationProgress, newProgress);
				double progressLength = (interpolationProgress - originalOldProgress);
				double interpolationProportion = progressLength / fullProgressDistance;
				double interpolationValueDistance = fullValueDistance * interpolationProportion;
				// estimates the value at the next [interpolated] subsegment boundary
				int interpolationValue = (int)interpolationValueDistance + originalOldValue;
				ExtendInternal(interpolationProgress, interpolationValue);
				AdvanceState(interpolationProgress, interpolationValue);
				values[closee] = (int)state.currentAccumulation;
				InitializeInterval();
			}
			ExtendInternal(newProgress, newValue);
			AdvanceState(newProgress, newValue);
			if (newIndex == count)
			{
				state = null;
			}
		}

		protected internal virtual void AdvanceState(double newProgress, int newValue)
		{
			state.oldValue = newValue;
			state.oldProgress = newProgress;
		}

		internal virtual int GetCount()
		{
			return count;
		}

		internal virtual int Get(int index)
		{
			return values[index];
		}
	}
}
