using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>A few more asserts</summary>
	public class MoreAsserts
	{
		/// <summary>Assert equivalence for array and iterable</summary>
		/// <?/>
		/// <param name="s">the name/message for the collection</param>
		/// <param name="expected">the expected array of elements</param>
		/// <param name="actual">the actual iterable of elements</param>
		public static void AssertEquals<T>(string s, T[] expected, IEnumerable<T> actual)
		{
			IEnumerator<T> it = actual.GetEnumerator();
			int i = 0;
			for (; i < expected.Length && it.HasNext(); ++i)
			{
				NUnit.Framework.Assert.AreEqual("Element " + i + " for " + s, expected[i], it.Next
					());
			}
			NUnit.Framework.Assert.IsTrue("Expected more elements", i == expected.Length);
			NUnit.Framework.Assert.IsTrue("Expected less elements", !it.HasNext());
		}

		/// <summary>Assert equality for two iterables</summary>
		/// <?/>
		/// <param name="s"/>
		/// <param name="expected"/>
		/// <param name="actual"/>
		public static void AssertEquals<T>(string s, IEnumerable<T> expected, IEnumerable
			<T> actual)
		{
			IEnumerator<T> ite = expected.GetEnumerator();
			IEnumerator<T> ita = actual.GetEnumerator();
			int i = 0;
			while (ite.HasNext() && ita.HasNext())
			{
				NUnit.Framework.Assert.AreEqual("Element " + i + " for " + s, ite.Next(), ita.Next
					());
			}
			NUnit.Framework.Assert.IsTrue("Expected more elements", !ite.HasNext());
			NUnit.Framework.Assert.IsTrue("Expected less elements", !ita.HasNext());
		}
	}
}
