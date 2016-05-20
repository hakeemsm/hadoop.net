using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	public class TestKerberosAuthenticationHandler : org.apache.hadoop.minikdc.KerberosSecurityTestcase
	{
		protected internal org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
			 handler;

		protected internal virtual org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
			 getNewAuthenticationHandler()
		{
			return new org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				();
		}

		protected internal virtual string getExpectedType()
		{
			return org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.TYPE;
		}

		protected internal virtual java.util.Properties getDefaultProperties()
		{
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.PRINCIPAL, org.apache.hadoop.security.authentication.KerberosTestUtils.getServerPrincipal
				());
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.KEYTAB, org.apache.hadoop.security.authentication.KerberosTestUtils.getKeytabFile
				());
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.NAME_RULES, "RULE:[1:$1@$0](.*@" + org.apache.hadoop.security.authentication.KerberosTestUtils
				.getRealm() + ")s/@.*//\n");
			return props;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			// create keytab
			java.io.File keytabFile = new java.io.File(org.apache.hadoop.security.authentication.KerberosTestUtils
				.getKeytabFile());
			string clientPrincipal = org.apache.hadoop.security.authentication.KerberosTestUtils
				.getClientPrincipal();
			string serverPrincipal = org.apache.hadoop.security.authentication.KerberosTestUtils
				.getServerPrincipal();
			clientPrincipal = Sharpen.Runtime.substring(clientPrincipal, 0, clientPrincipal.LastIndexOf
				("@"));
			serverPrincipal = Sharpen.Runtime.substring(serverPrincipal, 0, serverPrincipal.LastIndexOf
				("@"));
			getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
			// handler
			handler = getNewAuthenticationHandler();
			java.util.Properties props = getDefaultProperties();
			try
			{
				handler.init(props);
			}
			catch (System.Exception ex)
			{
				handler = null;
				throw;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testNameRules()
		{
			org.apache.hadoop.security.authentication.util.KerberosName kn = new org.apache.hadoop.security.authentication.util.KerberosName
				(org.apache.hadoop.security.authentication.KerberosTestUtils.getServerPrincipal(
				));
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.KerberosTestUtils
				.getRealm(), kn.getRealm());
			//destroy handler created in setUp()
			handler.destroy();
			org.apache.hadoop.security.authentication.util.KerberosName.setRules("RULE:[1:$1@$0](.*@FOO)s/@.*//\nDEFAULT"
				);
			handler = getNewAuthenticationHandler();
			java.util.Properties props = getDefaultProperties();
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.NAME_RULES, "RULE:[1:$1@$0](.*@BAR)s/@.*//\nDEFAULT");
			try
			{
				handler.init(props);
			}
			catch (System.Exception)
			{
			}
			kn = new org.apache.hadoop.security.authentication.util.KerberosName("bar@BAR");
			NUnit.Framework.Assert.AreEqual("bar", kn.getShortName());
			kn = new org.apache.hadoop.security.authentication.util.KerberosName("bar@FOO");
			try
			{
				kn.getShortName();
				NUnit.Framework.Assert.Fail();
			}
			catch (System.Exception)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testInit()
		{
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.KerberosTestUtils
				.getKeytabFile(), handler.getKeytab());
			System.Collections.Generic.ICollection<javax.security.auth.kerberos.KerberosPrincipal
				> principals = handler.getPrincipals();
			java.security.Principal expectedPrincipal = new javax.security.auth.kerberos.KerberosPrincipal
				(org.apache.hadoop.security.authentication.KerberosTestUtils.getServerPrincipal(
				));
			NUnit.Framework.Assert.IsTrue(principals.contains(expectedPrincipal));
			NUnit.Framework.Assert.AreEqual(1, principals.Count);
		}

		// dynamic configuration of HTTP principals
		/// <exception cref="System.Exception"/>
		public virtual void testDynamicPrincipalDiscovery()
		{
			string[] keytabUsers = new string[] { "HTTP/host1", "HTTP/host2", "HTTP2/host1", 
				"XHTTP/host" };
			string keytab = org.apache.hadoop.security.authentication.KerberosTestUtils.getKeytabFile
				();
			getKdc().createPrincipal(new java.io.File(keytab), keytabUsers);
			// destroy handler created in setUp()
			handler.destroy();
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.KEYTAB, keytab);
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.PRINCIPAL, "*");
			handler = getNewAuthenticationHandler();
			handler.init(props);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.KerberosTestUtils
				.getKeytabFile(), handler.getKeytab());
			System.Collections.Generic.ICollection<javax.security.auth.kerberos.KerberosPrincipal
				> loginPrincipals = handler.getPrincipals();
			foreach (string user in keytabUsers)
			{
				java.security.Principal principal = new javax.security.auth.kerberos.KerberosPrincipal
					(user + "@" + org.apache.hadoop.security.authentication.KerberosTestUtils.getRealm
					());
				bool expected = user.StartsWith("HTTP/");
				NUnit.Framework.Assert.AreEqual("checking for " + user, expected, loginPrincipals
					.contains(principal));
			}
		}

		// dynamic configuration of HTTP principals
		/// <exception cref="System.Exception"/>
		public virtual void testDynamicPrincipalDiscoveryMissingPrincipals()
		{
			string[] keytabUsers = new string[] { "hdfs/localhost" };
			string keytab = org.apache.hadoop.security.authentication.KerberosTestUtils.getKeytabFile
				();
			getKdc().createPrincipal(new java.io.File(keytab), keytabUsers);
			// destroy handler created in setUp()
			handler.destroy();
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.KEYTAB, keytab);
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.PRINCIPAL, "*");
			handler = getNewAuthenticationHandler();
			try
			{
				handler.init(props);
				NUnit.Framework.Assert.Fail("init should have failed");
			}
			catch (javax.servlet.ServletException ex)
			{
				NUnit.Framework.Assert.AreEqual("Principals do not exist in the keytab", ex.InnerException
					.Message);
			}
			catch (System.Exception t)
			{
				NUnit.Framework.Assert.Fail("wrong exception: " + t);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testType()
		{
			NUnit.Framework.Assert.AreEqual(getExpectedType(), handler.getType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRequestWithoutAuthorization()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			NUnit.Framework.Assert.IsNull(handler.authenticate(request, response));
			org.mockito.Mockito.verify(response).setHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.WWW_AUTHENTICATE, org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.NEGOTIATE);
			org.mockito.Mockito.verify(response).setStatus(javax.servlet.http.HttpServletResponse
				.SC_UNAUTHORIZED);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRequestWithInvalidAuthorization()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			org.mockito.Mockito.when(request.getHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.AUTHORIZATION)).thenReturn("invalid");
			NUnit.Framework.Assert.IsNull(handler.authenticate(request, response));
			org.mockito.Mockito.verify(response).setHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.WWW_AUTHENTICATE, org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.NEGOTIATE);
			org.mockito.Mockito.verify(response).setStatus(javax.servlet.http.HttpServletResponse
				.SC_UNAUTHORIZED);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRequestWithIncompleteAuthorization()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			org.mockito.Mockito.when(request.getHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.AUTHORIZATION)).thenReturn(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.NEGOTIATE);
			try
			{
				handler.authenticate(request, response);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException)
			{
			}
			catch (System.Exception)
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRequestWithAuthorization()
		{
			string token = org.apache.hadoop.security.authentication.KerberosTestUtils.doAsClient
				(new _Callable_225());
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			org.mockito.Mockito.when(request.getHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.AUTHORIZATION)).thenReturn(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.NEGOTIATE + " " + token);
			org.mockito.Mockito.when(request.getServerName()).thenReturn("localhost");
			org.apache.hadoop.security.authentication.server.AuthenticationToken authToken = 
				handler.authenticate(request, response);
			if (authToken != null)
			{
				org.mockito.Mockito.verify(response).setHeader(org.mockito.Mockito.eq(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.WWW_AUTHENTICATE), org.mockito.Mockito.matches(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.NEGOTIATE + " .*"));
				org.mockito.Mockito.verify(response).setStatus(javax.servlet.http.HttpServletResponse
					.SC_OK);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.KerberosTestUtils
					.getClientPrincipal(), authToken.getName());
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.security.authentication.KerberosTestUtils
					.getClientPrincipal().StartsWith(authToken.getUserName()));
				NUnit.Framework.Assert.AreEqual(getExpectedType(), authToken.getType());
			}
			else
			{
				org.mockito.Mockito.verify(response).setHeader(org.mockito.Mockito.eq(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.WWW_AUTHENTICATE), org.mockito.Mockito.matches(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.NEGOTIATE + " .*"));
				org.mockito.Mockito.verify(response).setStatus(javax.servlet.http.HttpServletResponse
					.SC_UNAUTHORIZED);
			}
		}

		private sealed class _Callable_225 : java.util.concurrent.Callable<string>
		{
			public _Callable_225()
			{
			}

			/// <exception cref="System.Exception"/>
			public string call()
			{
				org.ietf.jgss.GSSManager gssManager = org.ietf.jgss.GSSManager.getInstance();
				org.ietf.jgss.GSSContext gssContext = null;
				try
				{
					string servicePrincipal = org.apache.hadoop.security.authentication.KerberosTestUtils
						.getServerPrincipal();
					org.ietf.jgss.Oid oid = org.apache.hadoop.security.authentication.util.KerberosUtil
						.getOidInstance("NT_GSS_KRB5_PRINCIPAL");
					org.ietf.jgss.GSSName serviceName = gssManager.createName(servicePrincipal, oid);
					oid = org.apache.hadoop.security.authentication.util.KerberosUtil.getOidInstance(
						"GSS_KRB5_MECH_OID");
					gssContext = gssManager.createContext(serviceName, oid, null, org.ietf.jgss.GSSContext
						.DEFAULT_LIFETIME);
					gssContext.requestCredDeleg(true);
					gssContext.requestMutualAuth(true);
					byte[] inToken = new byte[0];
					byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.Length);
					org.apache.commons.codec.binary.Base64 base64 = new org.apache.commons.codec.binary.Base64
						(0);
					return base64.encodeToString(outToken);
				}
				finally
				{
					if (gssContext != null)
					{
						gssContext.dispose();
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRequestWithInvalidKerberosAuthorization()
		{
			string token = new org.apache.commons.codec.binary.Base64(0).encodeToString(new byte
				[] { 0, 1, 2 });
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			org.mockito.Mockito.when(request.getHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.AUTHORIZATION)).thenReturn(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.NEGOTIATE + token);
			try
			{
				handler.authenticate(request, response);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException)
			{
			}
			catch (System.Exception)
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			if (handler != null)
			{
				handler.destroy();
				handler = null;
			}
		}
	}
}
