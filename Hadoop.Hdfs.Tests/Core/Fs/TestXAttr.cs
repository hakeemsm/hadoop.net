using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Tests for <code>XAttr</code> objects.</summary>
	public class TestXAttr
	{
		private static XAttr Xattr;

		private static XAttr Xattr1;

		private static XAttr Xattr2;

		private static XAttr Xattr3;

		private static XAttr Xattr4;

		private static XAttr Xattr5;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			byte[] value = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
				(int)(0x33)) };
			Xattr = new XAttr.Builder().SetName("name").SetValue(value).Build();
			Xattr1 = new XAttr.Builder().SetNameSpace(XAttr.NameSpace.User).SetName("name").SetValue
				(value).Build();
			Xattr2 = new XAttr.Builder().SetNameSpace(XAttr.NameSpace.Trusted).SetName("name"
				).SetValue(value).Build();
			Xattr3 = new XAttr.Builder().SetNameSpace(XAttr.NameSpace.System).SetName("name")
				.SetValue(value).Build();
			Xattr4 = new XAttr.Builder().SetNameSpace(XAttr.NameSpace.Security).SetName("name"
				).SetValue(value).Build();
			Xattr5 = new XAttr.Builder().SetNameSpace(XAttr.NameSpace.Raw).SetName("name").SetValue
				(value).Build();
		}

		[NUnit.Framework.Test]
		public virtual void TestXAttrEquals()
		{
			NUnit.Framework.Assert.AreNotSame(Xattr1, Xattr2);
			NUnit.Framework.Assert.AreNotSame(Xattr2, Xattr3);
			NUnit.Framework.Assert.AreNotSame(Xattr3, Xattr4);
			NUnit.Framework.Assert.AreNotSame(Xattr4, Xattr5);
			NUnit.Framework.Assert.AreEqual(Xattr, Xattr1);
			NUnit.Framework.Assert.AreEqual(Xattr1, Xattr1);
			NUnit.Framework.Assert.AreEqual(Xattr2, Xattr2);
			NUnit.Framework.Assert.AreEqual(Xattr3, Xattr3);
			NUnit.Framework.Assert.AreEqual(Xattr4, Xattr4);
			NUnit.Framework.Assert.AreEqual(Xattr5, Xattr5);
			NUnit.Framework.Assert.IsFalse(Xattr1.Equals(Xattr2));
			NUnit.Framework.Assert.IsFalse(Xattr2.Equals(Xattr3));
			NUnit.Framework.Assert.IsFalse(Xattr3.Equals(Xattr4));
			NUnit.Framework.Assert.IsFalse(Xattr4.Equals(Xattr5));
		}

		[NUnit.Framework.Test]
		public virtual void TestXAttrHashCode()
		{
			NUnit.Framework.Assert.AreEqual(Xattr.GetHashCode(), Xattr1.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Xattr1.GetHashCode() == Xattr2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Xattr2.GetHashCode() == Xattr3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Xattr3.GetHashCode() == Xattr4.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Xattr4.GetHashCode() == Xattr5.GetHashCode());
		}
	}
}
