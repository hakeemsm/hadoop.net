using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Tests string interning
	/// <see cref="StringInterner"/>
	/// </summary>
	public class TestStringInterner
	{
		/// <summary>
		/// Test different references are returned for any of string
		/// instances that are equal to each other but not interned.
		/// </summary>
		[Fact]
		public virtual void TestNoIntern()
		{
			string literalABC = "ABC";
			string substringABC = Sharpen.Runtime.Substring("ABCDE", 0, 3);
			string heapABC = new string("ABC");
			NUnit.Framework.Assert.AreNotSame(literalABC, substringABC);
			NUnit.Framework.Assert.AreNotSame(literalABC, heapABC);
			NUnit.Framework.Assert.AreNotSame(substringABC, heapABC);
		}

		/// <summary>
		/// Test the same strong reference is returned for any
		/// of string instances that are equal to each other.
		/// </summary>
		[Fact]
		public virtual void TestStrongIntern()
		{
			string strongInternLiteralABC = StringInterner.StrongIntern("ABC");
			string strongInternSubstringABC = StringInterner.StrongIntern(Sharpen.Runtime.Substring
				("ABCDE", 0, 3));
			string strongInternHeapABC = StringInterner.StrongIntern(new string("ABC"));
			NUnit.Framework.Assert.AreSame(strongInternLiteralABC, strongInternSubstringABC);
			NUnit.Framework.Assert.AreSame(strongInternLiteralABC, strongInternHeapABC);
			NUnit.Framework.Assert.AreSame(strongInternSubstringABC, strongInternHeapABC);
		}

		/// <summary>
		/// Test the same weak reference is returned for any
		/// of string instances that are equal to each other.
		/// </summary>
		[Fact]
		public virtual void TestWeakIntern()
		{
			string weakInternLiteralABC = StringInterner.WeakIntern("ABC");
			string weakInternSubstringABC = StringInterner.WeakIntern(Sharpen.Runtime.Substring
				("ABCDE", 0, 3));
			string weakInternHeapABC = StringInterner.WeakIntern(new string("ABC"));
			NUnit.Framework.Assert.AreSame(weakInternLiteralABC, weakInternSubstringABC);
			NUnit.Framework.Assert.AreSame(weakInternLiteralABC, weakInternHeapABC);
			NUnit.Framework.Assert.AreSame(weakInternSubstringABC, weakInternHeapABC);
		}
	}
}
