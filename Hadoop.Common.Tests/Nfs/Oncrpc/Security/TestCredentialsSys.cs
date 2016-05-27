using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>
	/// Test for
	/// <see cref="CredentialsSys"/>
	/// </summary>
	public class TestCredentialsSys
	{
		[NUnit.Framework.Test]
		public virtual void TestReadWrite()
		{
			CredentialsSys credential = new CredentialsSys();
			credential.SetUID(0);
			credential.SetGID(1);
			XDR xdr = new XDR();
			credential.Write(xdr);
			CredentialsSys newCredential = new CredentialsSys();
			newCredential.Read(xdr.AsReadOnlyWrap());
			NUnit.Framework.Assert.AreEqual(0, newCredential.GetUID());
			NUnit.Framework.Assert.AreEqual(1, newCredential.GetGID());
		}
	}
}
