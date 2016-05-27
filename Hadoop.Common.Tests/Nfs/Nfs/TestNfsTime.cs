using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs
{
	public class TestNfsTime
	{
		[NUnit.Framework.Test]
		public virtual void TestConstructor()
		{
			NfsTime nfstime = new NfsTime(1001);
			NUnit.Framework.Assert.AreEqual(1, nfstime.GetSeconds());
			NUnit.Framework.Assert.AreEqual(1000000, nfstime.GetNseconds());
		}

		[NUnit.Framework.Test]
		public virtual void TestSerializeDeserialize()
		{
			// Serialize NfsTime
			NfsTime t1 = new NfsTime(1001);
			XDR xdr = new XDR();
			t1.Serialize(xdr);
			// Deserialize it back
			NfsTime t2 = NfsTime.Deserialize(xdr.AsReadOnlyWrap());
			// Ensure the NfsTimes are equal
			NUnit.Framework.Assert.AreEqual(t1, t2);
		}
	}
}
