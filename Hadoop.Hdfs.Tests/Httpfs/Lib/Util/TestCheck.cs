using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Util
{
	public class TestCheck : HTestCase
	{
		[NUnit.Framework.Test]
		public virtual void NotNullNotNull()
		{
			NUnit.Framework.Assert.AreEqual(Check.NotNull("value", "name"), "value");
		}

		public virtual void NotNullNull()
		{
			Check.NotNull(null, "name");
		}

		[NUnit.Framework.Test]
		public virtual void NotNullElementsNotNull()
		{
			Check.NotNullElements(new AList<string>(), "name");
			Check.NotNullElements(Arrays.AsList("a"), "name");
		}

		public virtual void NotNullElementsNullList()
		{
			Check.NotNullElements(null, "name");
		}

		public virtual void NotNullElementsNullElements()
		{
			Check.NotNullElements(Arrays.AsList("a", string.Empty, null), "name");
		}

		[NUnit.Framework.Test]
		public virtual void NotEmptyElementsNotNull()
		{
			Check.NotEmptyElements(new AList<string>(), "name");
			Check.NotEmptyElements(Arrays.AsList("a"), "name");
		}

		public virtual void NotEmptyElementsNullList()
		{
			Check.NotEmptyElements(null, "name");
		}

		public virtual void NotEmptyElementsNullElements()
		{
			Check.NotEmptyElements(Arrays.AsList("a", null), "name");
		}

		public virtual void NotEmptyElementsEmptyElements()
		{
			Check.NotEmptyElements(Arrays.AsList("a", string.Empty), "name");
		}

		[NUnit.Framework.Test]
		public virtual void NotEmptyNotEmtpy()
		{
			NUnit.Framework.Assert.AreEqual(Check.NotEmpty("value", "name"), "value");
		}

		public virtual void NotEmptyNull()
		{
			Check.NotEmpty(null, "name");
		}

		public virtual void NotEmptyEmpty()
		{
			Check.NotEmpty(string.Empty, "name");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void ValidIdentifierValid()
		{
			NUnit.Framework.Assert.AreEqual(Check.ValidIdentifier("a", 1, string.Empty), "a");
			NUnit.Framework.Assert.AreEqual(Check.ValidIdentifier("a1", 2, string.Empty), "a1"
				);
			NUnit.Framework.Assert.AreEqual(Check.ValidIdentifier("a_", 3, string.Empty), "a_"
				);
			NUnit.Framework.Assert.AreEqual(Check.ValidIdentifier("_", 1, string.Empty), "_");
		}

		/// <exception cref="System.Exception"/>
		public virtual void ValidIdentifierInvalid1()
		{
			Check.ValidIdentifier("!", 1, string.Empty);
		}

		/// <exception cref="System.Exception"/>
		public virtual void ValidIdentifierInvalid2()
		{
			Check.ValidIdentifier("a1", 1, string.Empty);
		}

		/// <exception cref="System.Exception"/>
		public virtual void ValidIdentifierInvalid3()
		{
			Check.ValidIdentifier("1", 1, string.Empty);
		}

		[NUnit.Framework.Test]
		public virtual void CheckGTZeroGreater()
		{
			NUnit.Framework.Assert.AreEqual(Check.Gt0(120, "test"), 120);
		}

		public virtual void CheckGTZeroZero()
		{
			Check.Gt0(0, "test");
		}

		public virtual void CheckGTZeroLessThanZero()
		{
			Check.Gt0(-1, "test");
		}

		[NUnit.Framework.Test]
		public virtual void CheckGEZero()
		{
			NUnit.Framework.Assert.AreEqual(Check.Ge0(120, "test"), 120);
			NUnit.Framework.Assert.AreEqual(Check.Ge0(0, "test"), 0);
		}

		public virtual void CheckGELessThanZero()
		{
			Check.Ge0(-1, "test");
		}
	}
}
