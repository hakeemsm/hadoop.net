using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Alias
{
	public class TestCredentialProvider
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCredentialEntry()
		{
			char[] key1 = new char[] { 1, 2, 3, 4 };
			CredentialProvider.CredentialEntry obj = new CredentialProvider.CredentialEntry("cred1"
				, key1);
			Assert.Equal("cred1", obj.GetAlias());
			Assert.AssertArrayEquals(new char[] { 1, 2, 3, 4 }, obj.GetCredential());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUnnestUri()
		{
			Assert.Equal(new Path("hdfs://nn.example.com/my/path"), ProviderUtils
				.UnnestUri(new URI("myscheme://hdfs@nn.example.com/my/path")));
			Assert.Equal(new Path("hdfs://nn/my/path?foo=bar&baz=bat#yyy")
				, ProviderUtils.UnnestUri(new URI("myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy"
				)));
			Assert.Equal(new Path("inner://hdfs@nn1.example.com/my/path"), 
				ProviderUtils.UnnestUri(new URI("outer://inner@hdfs@nn1.example.com/my/path")));
			Assert.Equal(new Path("user:///"), ProviderUtils.UnnestUri(new 
				URI("outer://user/")));
		}
	}
}
