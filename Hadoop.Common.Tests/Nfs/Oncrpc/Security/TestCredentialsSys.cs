using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>
	/// Test for
	/// <see cref="CredentialsSys"/>
	/// </summary>
	public class TestCredentialsSys
	{
		[Fact]
		public virtual void TestReadWrite()
		{
			CredentialsSys credential = new CredentialsSys();
			credential.SetUID(0);
			credential.SetGID(1);
			XDR xdr = new XDR();
			credential.Write(xdr);
			CredentialsSys newCredential = new CredentialsSys();
			newCredential.Read(xdr.AsReadOnlyWrap());
			Assert.Equal(0, newCredential.GetUID());
			Assert.Equal(1, newCredential.GetGID());
		}
	}
}
