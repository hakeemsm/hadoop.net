using System;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>A named counter that tracks the progress of a map/reduce job.</summary>
	/// <remarks>
	/// A named counter that tracks the progress of a map/reduce job.
	/// <p><code>Counters</code> represent global counters, defined either by the
	/// Map-Reduce framework or applications. Each <code>Counter</code> is named by
	/// an
	/// <see cref="Sharpen.Enum{E}"/>
	/// and has a long for the value.</p>
	/// <p><code>Counters</code> are bunched into Groups, each comprising of
	/// counters from a particular <code>Enum</code> class.
	/// </remarks>
	public interface Counter : Writable
	{
		/// <summary>Set the display name of the counter</summary>
		/// <param name="displayName">of the counter</param>
		[System.ObsoleteAttribute(@"(and no-op by default)")]
		void SetDisplayName(string displayName);

		/// <returns>the name of the counter</returns>
		string GetName();

		/// <summary>Get the display name of the counter.</summary>
		/// <returns>the user facing name of the counter</returns>
		string GetDisplayName();

		/// <summary>What is the current value of this counter?</summary>
		/// <returns>the current value</returns>
		long GetValue();

		/// <summary>Set this counter by the given value</summary>
		/// <param name="value">the value to set</param>
		void SetValue(long value);

		/// <summary>Increment this counter by the given value</summary>
		/// <param name="incr">the value to increase this counter by</param>
		void Increment(long incr);

		[InterfaceAudience.Private]
		Counter GetUnderlyingCounter();
	}
}
