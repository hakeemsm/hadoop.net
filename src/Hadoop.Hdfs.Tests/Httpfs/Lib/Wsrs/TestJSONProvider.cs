using System.IO;
using Org.Json.Simple;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public class TestJSONProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			JSONProvider p = new JSONProvider();
			NUnit.Framework.Assert.IsTrue(p.IsWriteable(typeof(JSONObject), null, null, null)
				);
			NUnit.Framework.Assert.IsFalse(p.IsWriteable(this.GetType(), null, null, null));
			NUnit.Framework.Assert.AreEqual(p.GetSize(null, null, null, null, null), -1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JSONObject json = new JSONObject();
			json["a"] = "A";
			p.WriteTo(json, typeof(JSONObject), null, null, null, null, baos);
			baos.Close();
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(baos.ToByteArray
				()).Trim(), "{\"a\":\"A\"}");
		}
	}
}
