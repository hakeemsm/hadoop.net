using System;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A facility for Map-Reduce applications to report progress and update
	/// counters, status information etc.
	/// </summary>
	/// <remarks>
	/// A facility for Map-Reduce applications to report progress and update
	/// counters, status information etc.
	/// <p>
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// and
	/// <see cref="Reducer{K2, V2, K3, V3}"/>
	/// can use the <code>Reporter</code>
	/// provided to report progress or just indicate that they are alive. In
	/// scenarios where the application takes significant amount of time to
	/// process individual key/value pairs, this is crucial since the framework
	/// might assume that the task has timed-out and kill that task.
	/// <p>Applications can also update
	/// <see cref="Counters"/>
	/// via the provided
	/// <code>Reporter</code> .</p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Util.Progressable"/>
	/// <seealso cref="Counters"/>
	public abstract class Reporter : Progressable
	{
		private sealed class _Reporter_49 : Reporter
		{
			public _Reporter_49()
			{
			}

			public override void SetStatus(string s)
			{
			}

			public void Progress()
			{
			}

			public override Counters.Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return null;
			}

			public override Counters.Counter GetCounter(string group, string name)
			{
				return null;
			}

			public override void IncrCounter<_T0>(Enum<_T0> key, long amount)
			{
			}

			public override void IncrCounter(string group, string counter, long amount)
			{
			}

			/// <exception cref="System.NotSupportedException"/>
			public override InputSplit GetInputSplit()
			{
				throw new NotSupportedException("NULL reporter has no input");
			}

			public override float GetProgress()
			{
				return 0;
			}
		}

		/// <summary>A constant of Reporter type that does nothing.</summary>
		public const Reporter Null = new _Reporter_49();

		/// <summary>Set the status description for the task.</summary>
		/// <param name="status">brief description of the current status.</param>
		public abstract void SetStatus(string status);

		/// <summary>
		/// Get the
		/// <see cref="Counter"/>
		/// of the given group with the given name.
		/// </summary>
		/// <param name="name">counter name</param>
		/// <returns>the <code>Counter</code> of the given group/name.</returns>
		public abstract Counters.Counter GetCounter<_T0>(Enum<_T0> name)
			where _T0 : Enum<E>;

		/// <summary>
		/// Get the
		/// <see cref="Counter"/>
		/// of the given group with the given name.
		/// </summary>
		/// <param name="group">counter group</param>
		/// <param name="name">counter name</param>
		/// <returns>the <code>Counter</code> of the given group/name.</returns>
		public abstract Counters.Counter GetCounter(string group, string name);

		/// <summary>
		/// Increments the counter identified by the key, which can be of
		/// any
		/// <see cref="Sharpen.Enum{E}"/>
		/// type, by the specified amount.
		/// </summary>
		/// <param name="key">
		/// key to identify the counter to be incremented. The key can be
		/// be any <code>Enum</code>.
		/// </param>
		/// <param name="amount">
		/// A non-negative amount by which the counter is to
		/// be incremented.
		/// </param>
		public abstract void IncrCounter<_T0>(Enum<_T0> key, long amount)
			where _T0 : Enum<E>;

		/// <summary>
		/// Increments the counter identified by the group and counter name
		/// by the specified amount.
		/// </summary>
		/// <param name="group">name to identify the group of the counter to be incremented.</param>
		/// <param name="counter">name to identify the counter within the group.</param>
		/// <param name="amount">
		/// A non-negative amount by which the counter is to
		/// be incremented.
		/// </param>
		public abstract void IncrCounter(string group, string counter, long amount);

		/// <summary>
		/// Get the
		/// <see cref="InputSplit"/>
		/// object for a map.
		/// </summary>
		/// <returns>the <code>InputSplit</code> that the map is reading from.</returns>
		/// <exception cref="System.NotSupportedException">if called outside a mapper</exception>
		public abstract InputSplit GetInputSplit();

		/// <summary>Get the progress of the task.</summary>
		/// <remarks>
		/// Get the progress of the task. Progress is represented as a number between
		/// 0 and 1 (inclusive).
		/// </remarks>
		public abstract float GetProgress();
	}

	public static class ReporterConstants
	{
	}
}
