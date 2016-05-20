using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Provides equivalent behavior to String.intern() to optimize performance,
	/// whereby does not consume memory in the permanent generation.
	/// </summary>
	public class StringInterner
	{
		/// <summary>Retains a strong reference to each string instance it has interned.</summary>
		private static readonly com.google.common.collect.Interner<string> strongInterner;

		/// <summary>Retains a weak reference to each string instance it has interned.</summary>
		private static readonly com.google.common.collect.Interner<string> weakInterner;

		static StringInterner()
		{
			strongInterner = com.google.common.collect.Interners.newStrongInterner();
			weakInterner = com.google.common.collect.Interners.newWeakInterner();
		}

		/// <summary>
		/// Interns and returns a reference to the representative instance
		/// for any of a collection of string instances that are equal to each other.
		/// </summary>
		/// <remarks>
		/// Interns and returns a reference to the representative instance
		/// for any of a collection of string instances that are equal to each other.
		/// Retains strong reference to the instance,
		/// thus preventing it from being garbage-collected.
		/// </remarks>
		/// <param name="sample">string instance to be interned</param>
		/// <returns>strong reference to interned string instance</returns>
		public static string strongIntern(string sample)
		{
			if (sample == null)
			{
				return null;
			}
			return strongInterner.intern(sample);
		}

		/// <summary>
		/// Interns and returns a reference to the representative instance
		/// for any of a collection of string instances that are equal to each other.
		/// </summary>
		/// <remarks>
		/// Interns and returns a reference to the representative instance
		/// for any of a collection of string instances that are equal to each other.
		/// Retains weak reference to the instance,
		/// and so does not prevent it from being garbage-collected.
		/// </remarks>
		/// <param name="sample">string instance to be interned</param>
		/// <returns>weak reference to interned string instance</returns>
		public static string weakIntern(string sample)
		{
			if (sample == null)
			{
				return null;
			}
			return weakInterner.intern(sample);
		}
	}
}
