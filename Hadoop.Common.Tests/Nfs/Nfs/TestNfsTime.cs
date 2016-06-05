using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs
{
	public class TestNfsTime
	{
		[Fact]
		public virtual void TestConstructor()
		{
			NfsTime nfstime = new NfsTime(1001);
			Assert.Equal(1, nfstime.GetSeconds());
			Assert.Equal(1000000, nfstime.GetNseconds());
		}

		[Fact]
		public virtual void TestSerializeDeserialize()
		{
			// Serialize NfsTime
			NfsTime t1 = new NfsTime(1001);
			XDR xdr = new XDR();
			t1.Serialize(xdr);
			// Deserialize it back
			NfsTime t2 = NfsTime.Deserialize(xdr.AsReadOnlyWrap());
			// Ensure the NfsTimes are equal
			Assert.Equal(t1, t2);
		}
	}
}
