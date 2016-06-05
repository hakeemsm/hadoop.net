using System;
using System.IO;
using System.Net;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	public class TestJspHelper
	{
		private readonly Configuration conf = new HdfsConfiguration();

		// allow user with TGT to run tests
		[BeforeClass]
		public static void SetupKerb()
		{
			Runtime.SetProperty("java.security.krb5.kdc", string.Empty);
			Runtime.SetProperty("java.security.krb5.realm", "NONE");
		}

		public class DummySecretManager : AbstractDelegationTokenSecretManager<DelegationTokenIdentifier
			>
		{
			public DummySecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
				, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval)
				: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
					, delegationTokenRemoverScanInterval)
			{
			}

			public override DelegationTokenIdentifier CreateIdentifier()
			{
				return null;
			}

			protected override byte[] CreatePassword(DelegationTokenIdentifier dtId)
			{
				return new byte[1];
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetUgi()
		{
			conf.Set(DFSConfigKeys.FsDefaultNameKey, "hdfs://localhost:4321/");
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			string user = "TheDoctor";
			Text userText = new Text(user);
			DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(userText, userText
				, null);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId, new TestJspHelper.DummySecretManager(0, 0, 0, 
				0));
			string tokenString = token.EncodeToUrlString();
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			Org.Mockito.Mockito.When(request.GetRemoteUser()).ThenReturn(user);
			//Test attribute in the url to be used as service in the token.
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.NamenodeAddress)).ThenReturn
				("1.1.1.1:1111");
			conf.Set(DFSConfigKeys.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			VerifyServiceInToken(context, request, "1.1.1.1:1111");
			//Test attribute name.node.address 
			//Set the nnaddr url parameter to null.
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.NamenodeAddress)).ThenReturn
				(null);
			IPEndPoint addr = new IPEndPoint("localhost", 2222);
			Org.Mockito.Mockito.When(context.GetAttribute(NameNodeHttpServer.NamenodeAddressAttributeKey
				)).ThenReturn(addr);
			VerifyServiceInToken(context, request, addr.Address.GetHostAddress() + ":2222");
			//Test service already set in the token
			token.SetService(new Text("3.3.3.3:3333"));
			tokenString = token.EncodeToUrlString();
			//Set the name.node.address attribute in Servlet context to null
			Org.Mockito.Mockito.When(context.GetAttribute(NameNodeHttpServer.NamenodeAddressAttributeKey
				)).ThenReturn(null);
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			VerifyServiceInToken(context, request, "3.3.3.3:3333");
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyServiceInToken(ServletContext context, HttpServletRequest request
			, string expected)
		{
			UserGroupInformation ugi = JspHelper.GetUGI(context, request, conf);
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> tokenInUgi = ugi.GetTokens
				().GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(expected, tokenInUgi.GetService().ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetUgiFromToken()
		{
			conf.Set(DFSConfigKeys.FsDefaultNameKey, "hdfs://localhost:4321/");
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			string realUser = "TheDoctor";
			string user = "TheNurse";
			conf.Set(DFSConfigKeys.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation ugi;
			HttpServletRequest request;
			Text ownerText = new Text(user);
			DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(ownerText, ownerText
				, new Text(realUser));
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId, new TestJspHelper.DummySecretManager(0, 0, 0, 
				0));
			string tokenString = token.EncodeToUrlString();
			// token with no auth-ed user
			request = GetMockRequest(null, null, null);
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetRealUser().GetShortUserName(), realUser);
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), user);
			CheckUgiFromToken(ugi);
			// token with auth-ed user
			request = GetMockRequest(realUser, null, null);
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetRealUser().GetShortUserName(), realUser);
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), user);
			CheckUgiFromToken(ugi);
			// completely different user, token trumps auth
			request = GetMockRequest("rogue", null, null);
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetRealUser().GetShortUserName(), realUser);
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), user);
			CheckUgiFromToken(ugi);
			// expected case
			request = GetMockRequest(null, user, null);
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetRealUser().GetShortUserName(), realUser);
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), user);
			CheckUgiFromToken(ugi);
			// can't proxy with a token!
			request = GetMockRequest(null, null, "rogue");
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Usernames not matched: name=rogue != expected=" 
					+ user, ioe.Message);
			}
			// can't proxy with a token!
			request = GetMockRequest(null, user, "rogue");
			Org.Mockito.Mockito.When(request.GetParameter(JspHelper.DelegationParameterName))
				.ThenReturn(tokenString);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Usernames not matched: name=rogue != expected=" 
					+ user, ioe.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNonProxyUgi()
		{
			conf.Set(DFSConfigKeys.FsDefaultNameKey, "hdfs://localhost:4321/");
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			string realUser = "TheDoctor";
			string user = "TheNurse";
			conf.Set(DFSConfigKeys.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation ugi;
			HttpServletRequest request;
			// have to be auth-ed with remote user
			request = GetMockRequest(null, null, null);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Security enabled but user not authenticated by filter"
					, ioe.Message);
			}
			request = GetMockRequest(null, realUser, null);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Security enabled but user not authenticated by filter"
					, ioe.Message);
			}
			// ugi for remote user
			request = GetMockRequest(realUser, null, null);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), realUser);
			CheckUgiFromAuth(ugi);
			// ugi for remote user = real user
			request = GetMockRequest(realUser, realUser, null);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), realUser);
			CheckUgiFromAuth(ugi);
			// ugi for remote user != real user 
			request = GetMockRequest(realUser, user, null);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Usernames not matched: name=" + user + " != expected="
					 + realUser, ioe.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetProxyUgi()
		{
			conf.Set(DFSConfigKeys.FsDefaultNameKey, "hdfs://localhost:4321/");
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			string realUser = "TheDoctor";
			string user = "TheNurse";
			conf.Set(DFSConfigKeys.HadoopSecurityAuthentication, "kerberos");
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(realUser), "*");
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(realUser), "*");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation ugi;
			HttpServletRequest request;
			// have to be auth-ed with remote user
			request = GetMockRequest(null, null, user);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Security enabled but user not authenticated by filter"
					, ioe.Message);
			}
			request = GetMockRequest(null, realUser, user);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Security enabled but user not authenticated by filter"
					, ioe.Message);
			}
			// proxy ugi for user via remote user
			request = GetMockRequest(realUser, null, user);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetRealUser().GetShortUserName(), realUser);
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), user);
			CheckUgiFromAuth(ugi);
			// proxy ugi for user vi a remote user = real user
			request = GetMockRequest(realUser, realUser, user);
			ugi = JspHelper.GetUGI(context, request, conf);
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual(ugi.GetRealUser().GetShortUserName(), realUser);
			NUnit.Framework.Assert.AreEqual(ugi.GetShortUserName(), user);
			CheckUgiFromAuth(ugi);
			// proxy ugi for user via remote user != real user
			request = GetMockRequest(realUser, user, user);
			try
			{
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad request allowed");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Usernames not matched: name=" + user + " != expected="
					 + realUser, ioe.Message);
			}
			// try to get get a proxy user with unauthorized user
			try
			{
				request = GetMockRequest(user, null, realUser);
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad proxy request allowed");
			}
			catch (AuthorizationException ae)
			{
				NUnit.Framework.Assert.AreEqual("User: " + user + " is not allowed to impersonate "
					 + realUser, ae.Message);
			}
			try
			{
				request = GetMockRequest(user, user, realUser);
				JspHelper.GetUGI(context, request, conf);
				NUnit.Framework.Assert.Fail("bad proxy request allowed");
			}
			catch (AuthorizationException ae)
			{
				NUnit.Framework.Assert.AreEqual("User: " + user + " is not allowed to impersonate "
					 + realUser, ae.Message);
			}
		}

		private HttpServletRequest GetMockRequest(string remoteUser, string user, string 
			doAs)
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetParameter(UserParam.Name)).ThenReturn(user);
			if (doAs != null)
			{
				Org.Mockito.Mockito.When(request.GetParameter(DoAsParam.Name)).ThenReturn(doAs);
			}
			Org.Mockito.Mockito.When(request.GetRemoteUser()).ThenReturn(remoteUser);
			return request;
		}

		private void CheckUgiFromAuth(UserGroupInformation ugi)
		{
			if (ugi.GetRealUser() != null)
			{
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Proxy, 
					ugi.GetAuthenticationMethod());
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.KerberosSsl
					, ugi.GetRealUser().GetAuthenticationMethod());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.KerberosSsl
					, ugi.GetAuthenticationMethod());
			}
		}

		private void CheckUgiFromToken(UserGroupInformation ugi)
		{
			if (ugi.GetRealUser() != null)
			{
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Proxy, 
					ugi.GetAuthenticationMethod());
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Token, 
					ugi.GetRealUser().GetAuthenticationMethod());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Token, 
					ugi.GetAuthenticationMethod());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestReadWriteReplicaState()
		{
			try
			{
				DataOutputBuffer @out = new DataOutputBuffer();
				DataInputBuffer @in = new DataInputBuffer();
				foreach (HdfsServerConstants.ReplicaState repState in HdfsServerConstants.ReplicaState
					.Values())
				{
					repState.Write(@out);
					@in.Reset(@out.GetData(), @out.GetLength());
					HdfsServerConstants.ReplicaState result = HdfsServerConstants.ReplicaState.Read(@in
						);
					NUnit.Framework.Assert.IsTrue("testReadWrite error !!!", repState == result);
					@out.Reset();
					@in.Reset();
				}
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testReadWrite ex error ReplicaState");
			}
		}

		private static string clientAddr = "1.1.1.1";

		private static string chainedClientAddr = clientAddr + ", 2.2.2.2";

		private static string proxyAddr = "3.3.3.3";

		[NUnit.Framework.Test]
		public virtual void TestRemoteAddr()
		{
			NUnit.Framework.Assert.AreEqual(clientAddr, GetRemoteAddr(clientAddr, null, false
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteAddrWithUntrustedProxy()
		{
			NUnit.Framework.Assert.AreEqual(proxyAddr, GetRemoteAddr(clientAddr, proxyAddr, false
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteAddrWithTrustedProxy()
		{
			NUnit.Framework.Assert.AreEqual(clientAddr, GetRemoteAddr(clientAddr, proxyAddr, 
				true));
			NUnit.Framework.Assert.AreEqual(clientAddr, GetRemoteAddr(chainedClientAddr, proxyAddr
				, true));
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteAddrWithTrustedProxyAndEmptyClient()
		{
			NUnit.Framework.Assert.AreEqual(proxyAddr, GetRemoteAddr(null, proxyAddr, true));
			NUnit.Framework.Assert.AreEqual(proxyAddr, GetRemoteAddr(string.Empty, proxyAddr, 
				true));
		}

		private string GetRemoteAddr(string clientAddr, string proxyAddr, bool trusted)
		{
			HttpServletRequest req = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(req.GetRemoteAddr()).ThenReturn("1.2.3.4");
			Configuration conf = new Configuration();
			if (proxyAddr == null)
			{
				Org.Mockito.Mockito.When(req.GetRemoteAddr()).ThenReturn(clientAddr);
			}
			else
			{
				Org.Mockito.Mockito.When(req.GetRemoteAddr()).ThenReturn(proxyAddr);
				Org.Mockito.Mockito.When(req.GetHeader("X-Forwarded-For")).ThenReturn(clientAddr);
				if (trusted)
				{
					conf.Set(ProxyServers.ConfHadoopProxyservers, proxyAddr);
				}
			}
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			return JspHelper.GetRemoteAddr(req);
		}
	}
}
