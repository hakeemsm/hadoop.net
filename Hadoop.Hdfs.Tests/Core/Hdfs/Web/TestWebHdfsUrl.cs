using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestWebHdfsUrl
	{
		internal readonly URI uri = URI.Create(WebHdfsFileSystem.Scheme + "://" + "127.0.0.1:0"
			);

		// NOTE: port is never used 
		[SetUp]
		public virtual void ResetUGI()
		{
			UserGroupInformation.SetConfiguration(new Configuration());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void TestEncodedPathUrl()
		{
			Configuration conf = new Configuration();
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)FileSystem.Get(uri, conf);
			// Construct a file path that contains percentage-encoded string
			string pathName = "/hdtest010%2C60020%2C1371000602151.1371058984668";
			Path fsPath = new Path(pathName);
			Uri encodedPathUrl = webhdfs.ToUrl(PutOpParam.OP.Create, fsPath);
			// We should get back the original file path after cycling back and decoding
			NUnit.Framework.Assert.AreEqual(WebHdfsFileSystem.PathPrefix + pathName, encodedPathUrl
				.ToURI().GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSimpleAuthParamsInUrl()
		{
			Configuration conf = new Configuration();
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test-user");
			UserGroupInformation.SetLoginUser(ugi);
			WebHdfsFileSystem webhdfs = GetWebHdfsFileSystem(ugi, conf);
			Path fsPath = new Path("/");
			// send user+token
			Uri fileStatusUrl = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getfilestatus.ToQueryString(), new 
				UserParam(ugi.GetShortUserName()).ToString() }, fileStatusUrl);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSimpleProxyAuthParamsInUrl()
		{
			Configuration conf = new Configuration();
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test-user");
			ugi = UserGroupInformation.CreateProxyUser("test-proxy-user", ugi);
			UserGroupInformation.SetLoginUser(ugi);
			WebHdfsFileSystem webhdfs = GetWebHdfsFileSystem(ugi, conf);
			Path fsPath = new Path("/");
			// send real+effective
			Uri fileStatusUrl = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getfilestatus.ToQueryString(), new 
				UserParam(ugi.GetRealUser().GetShortUserName()).ToString(), new DoAsParam(ugi.GetShortUserName
				()).ToString() }, fileStatusUrl);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSecureAuthParamsInUrl()
		{
			Configuration conf = new Configuration();
			// fake turning on security so api thinks it should use tokens
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test-user");
			ugi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos);
			UserGroupInformation.SetLoginUser(ugi);
			WebHdfsFileSystem webhdfs = GetWebHdfsFileSystem(ugi, conf);
			Path fsPath = new Path("/");
			string tokenString = webhdfs.GetDelegationToken().EncodeToUrlString();
			// send user
			Uri getTokenUrl = webhdfs.ToUrl(GetOpParam.OP.Getdelegationtoken, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getdelegationtoken.ToQueryString(), 
				new UserParam(ugi.GetShortUserName()).ToString() }, getTokenUrl);
			// send user
			Uri renewTokenUrl = webhdfs.ToUrl(PutOpParam.OP.Renewdelegationtoken, fsPath, new 
				TokenArgumentParam(tokenString));
			CheckQueryParams(new string[] { PutOpParam.OP.Renewdelegationtoken.ToQueryString(
				), new UserParam(ugi.GetShortUserName()).ToString(), new TokenArgumentParam(tokenString
				).ToString() }, renewTokenUrl);
			// send token
			Uri cancelTokenUrl = webhdfs.ToUrl(PutOpParam.OP.Canceldelegationtoken, fsPath, new 
				TokenArgumentParam(tokenString));
			CheckQueryParams(new string[] { PutOpParam.OP.Canceldelegationtoken.ToQueryString
				(), new UserParam(ugi.GetShortUserName()).ToString(), new TokenArgumentParam(tokenString
				).ToString() }, cancelTokenUrl);
			// send token
			Uri fileStatusUrl = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getfilestatus.ToQueryString(), new 
				DelegationParam(tokenString).ToString() }, fileStatusUrl);
			// wipe out internal token to simulate auth always required
			webhdfs.SetDelegationToken(null);
			// send user
			cancelTokenUrl = webhdfs.ToUrl(PutOpParam.OP.Canceldelegationtoken, fsPath, new TokenArgumentParam
				(tokenString));
			CheckQueryParams(new string[] { PutOpParam.OP.Canceldelegationtoken.ToQueryString
				(), new UserParam(ugi.GetShortUserName()).ToString(), new TokenArgumentParam(tokenString
				).ToString() }, cancelTokenUrl);
			// send user
			fileStatusUrl = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getfilestatus.ToQueryString(), new 
				UserParam(ugi.GetShortUserName()).ToString() }, fileStatusUrl);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSecureProxyAuthParamsInUrl()
		{
			Configuration conf = new Configuration();
			// fake turning on security so api thinks it should use tokens
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test-user");
			ugi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos);
			ugi = UserGroupInformation.CreateProxyUser("test-proxy-user", ugi);
			UserGroupInformation.SetLoginUser(ugi);
			WebHdfsFileSystem webhdfs = GetWebHdfsFileSystem(ugi, conf);
			Path fsPath = new Path("/");
			string tokenString = webhdfs.GetDelegationToken().EncodeToUrlString();
			// send real+effective
			Uri getTokenUrl = webhdfs.ToUrl(GetOpParam.OP.Getdelegationtoken, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getdelegationtoken.ToQueryString(), 
				new UserParam(ugi.GetRealUser().GetShortUserName()).ToString(), new DoAsParam(ugi
				.GetShortUserName()).ToString() }, getTokenUrl);
			// send real+effective
			Uri renewTokenUrl = webhdfs.ToUrl(PutOpParam.OP.Renewdelegationtoken, fsPath, new 
				TokenArgumentParam(tokenString));
			CheckQueryParams(new string[] { PutOpParam.OP.Renewdelegationtoken.ToQueryString(
				), new UserParam(ugi.GetRealUser().GetShortUserName()).ToString(), new DoAsParam
				(ugi.GetShortUserName()).ToString(), new TokenArgumentParam(tokenString).ToString
				() }, renewTokenUrl);
			// send token
			Uri cancelTokenUrl = webhdfs.ToUrl(PutOpParam.OP.Canceldelegationtoken, fsPath, new 
				TokenArgumentParam(tokenString));
			CheckQueryParams(new string[] { PutOpParam.OP.Canceldelegationtoken.ToQueryString
				(), new UserParam(ugi.GetRealUser().GetShortUserName()).ToString(), new DoAsParam
				(ugi.GetShortUserName()).ToString(), new TokenArgumentParam(tokenString).ToString
				() }, cancelTokenUrl);
			// send token
			Uri fileStatusUrl = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getfilestatus.ToQueryString(), new 
				DelegationParam(tokenString).ToString() }, fileStatusUrl);
			// wipe out internal token to simulate auth always required
			webhdfs.SetDelegationToken(null);
			// send real+effective
			cancelTokenUrl = webhdfs.ToUrl(PutOpParam.OP.Canceldelegationtoken, fsPath, new TokenArgumentParam
				(tokenString));
			CheckQueryParams(new string[] { PutOpParam.OP.Canceldelegationtoken.ToQueryString
				(), new UserParam(ugi.GetRealUser().GetShortUserName()).ToString(), new DoAsParam
				(ugi.GetShortUserName()).ToString(), new TokenArgumentParam(tokenString).ToString
				() }, cancelTokenUrl);
			// send real+effective
			fileStatusUrl = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, fsPath);
			CheckQueryParams(new string[] { GetOpParam.OP.Getfilestatus.ToQueryString(), new 
				UserParam(ugi.GetRealUser().GetShortUserName()).ToString(), new DoAsParam(ugi.GetShortUserName
				()).ToString() }, fileStatusUrl);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCheckAccessUrl()
		{
			Configuration conf = new Configuration();
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test-user");
			UserGroupInformation.SetLoginUser(ugi);
			WebHdfsFileSystem webhdfs = GetWebHdfsFileSystem(ugi, conf);
			Path fsPath = new Path("/p1");
			Uri checkAccessUrl = webhdfs.ToUrl(GetOpParam.OP.Checkaccess, fsPath, new FsActionParam
				(FsAction.ReadWrite));
			CheckQueryParams(new string[] { GetOpParam.OP.Checkaccess.ToQueryString(), new UserParam
				(ugi.GetShortUserName()).ToString(), FsActionParam.Name + "=" + FsAction.ReadWrite
				.Symbol }, checkAccessUrl);
		}

		private void CheckQueryParams(string[] expected, Uri url)
		{
			Arrays.Sort(expected);
			string[] query = url.GetQuery().Split("&");
			Arrays.Sort(query);
			NUnit.Framework.Assert.AreEqual(Arrays.ToString(expected), Arrays.ToString(query)
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private WebHdfsFileSystem GetWebHdfsFileSystem(UserGroupInformation ugi, Configuration
			 conf)
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(ugi.GetUserName
					()), null, null);
				FSNamesystem namesystem = Org.Mockito.Mockito.Mock<FSNamesystem>();
				DelegationTokenSecretManager dtSecretManager = new DelegationTokenSecretManager(86400000
					, 86400000, 86400000, 86400000, namesystem);
				dtSecretManager.StartThreads();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>(dtId, dtSecretManager);
				SecurityUtil.SetTokenService(token, NetUtils.CreateSocketAddr(uri.GetAuthority())
					);
				token.SetKind(WebHdfsFileSystem.TokenKind);
				ugi.AddToken(token);
			}
			return (WebHdfsFileSystem)FileSystem.Get(uri, conf);
		}
	}
}
