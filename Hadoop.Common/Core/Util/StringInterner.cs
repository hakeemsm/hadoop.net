using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Provides equivalent behavior to String.intern() to optimize performance,
	/// whereby does not consume memory in the permanent generation.
	/// </summary>
	public class StringInterner
	{
		/// <summary>Retains a strong reference to each string instance it has interned.</summary>
		private static readonly Interner<string> strongInterner;

		/// <summary>Retains a weak reference to each string instance it has interned.</summary>
		private static readonly Interner<string> weakInterner;

		static StringInterner()
		{
			strongInterner = Interners.NewStrongInterner();
			weakInterner = Interners.NewWeakInterner();
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
		public static string StrongIntern(string sample)
		{
			if (sample == null)
			{
				return null;
			}
			return strongInterner.Intern(sample);
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
		public static string WeakIntern(string sample)
		{
			if (sample == null)
			{
				return null;
			}
			return weakInterner.Intern(sample);
		}
	}
}
