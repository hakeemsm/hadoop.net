using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestHftpDelegationToken
	{
		/// <summary>
		/// Test whether HftpFileSystem maintain wire-compatibility for 0.20.203 when
		/// obtaining delegation token.
		/// </summary>
		/// <remarks>
		/// Test whether HftpFileSystem maintain wire-compatibility for 0.20.203 when
		/// obtaining delegation token. See HDFS-5440 for more details.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenCompatibilityFor203()
		{
			Configuration conf = new Configuration();
			HftpFileSystem fs = new HftpFileSystem();
			Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>(new byte[0], new byte[0], DelegationTokenIdentifier.HdfsDelegationKind
				, new Text("127.0.0.1:8020"));
			Credentials cred = new Credentials();
			cred.AddToken(HftpFileSystem.TokenKind, token);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			cred.Write(new DataOutputStream(os));
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.DoReturn(new ByteArrayInputStream(os.ToByteArray())).When(conn
				).GetInputStream();
			Org.Mockito.Mockito.DoReturn(HttpURLConnection.HttpOk).When(conn).GetResponseCode
				();
			URLConnectionFactory factory = Org.Mockito.Mockito.Mock<URLConnectionFactory>();
			Org.Mockito.Mockito.DoReturn(conn).When(factory).OpenConnection(Org.Mockito.Mockito
				.Any<Uri>(), Matchers.AnyBoolean());
			URI uri = new URI("hftp://127.0.0.1:8020");
			fs.Initialize(uri, conf);
			fs.connectionFactory = factory;
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("foo", new string
				[] { "bar" });
			TokenAspect<HftpFileSystem> tokenAspect = new TokenAspect<HftpFileSystem>(fs, SecurityUtil
				.BuildTokenService(uri), HftpFileSystem.TokenKind);
			tokenAspect.InitDelegationToken(ugi);
			tokenAspect.EnsureTokenInitialized();
			NUnit.Framework.Assert.AreSame(HftpFileSystem.TokenKind, fs.GetRenewToken().GetKind
				());
			Org.Apache.Hadoop.Security.Token.Token<object> tok = (Org.Apache.Hadoop.Security.Token.Token
				<object>)Whitebox.GetInternalState(fs, "delegationToken");
			NUnit.Framework.Assert.AreNotSame("Not making a copy of the remote token", token, 
				tok);
			NUnit.Framework.Assert.AreEqual(token.GetKind(), tok.GetKind());
		}
	}
}
