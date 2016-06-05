using System;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Lang
{
	public class TestXException : HTestCase
	{
		[System.Serializable]
		public sealed class TestERROR : XException.ERROR
		{
			public static readonly TestXException.TestERROR Tc = new TestXException.TestERROR
				();

			public string GetTemplate()
			{
				return "{0}";
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestXException()
		{
			XException ex = new XException(TestXException.TestERROR.Tc);
			NUnit.Framework.Assert.AreEqual(ex.GetError(), TestXException.TestERROR.Tc);
			NUnit.Framework.Assert.AreEqual(ex.Message, "TC: {0}");
			NUnit.Framework.Assert.IsNull(ex.InnerException);
			ex = new XException(TestXException.TestERROR.Tc, "msg");
			NUnit.Framework.Assert.AreEqual(ex.GetError(), TestXException.TestERROR.Tc);
			NUnit.Framework.Assert.AreEqual(ex.Message, "TC: msg");
			NUnit.Framework.Assert.IsNull(ex.InnerException);
			Exception cause = new Exception();
			ex = new XException(TestXException.TestERROR.Tc, cause);
			NUnit.Framework.Assert.AreEqual(ex.GetError(), TestXException.TestERROR.Tc);
			NUnit.Framework.Assert.AreEqual(ex.Message, "TC: " + cause.ToString());
			NUnit.Framework.Assert.AreEqual(ex.InnerException, cause);
			XException xcause = ex;
			ex = new XException(xcause);
			NUnit.Framework.Assert.AreEqual(ex.GetError(), TestXException.TestERROR.Tc);
			NUnit.Framework.Assert.AreEqual(ex.Message, xcause.Message);
			NUnit.Framework.Assert.AreEqual(ex.InnerException, xcause);
		}
	}
}
