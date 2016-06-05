using System;
using System.Collections.Generic;
using System.IO;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	public class TestUserGroupInformation
	{
		private const string UserName = "user1@HADOOP.APACHE.ORG";

		private const string Group1Name = "group1";

		private const string Group2Name = "group2";

		private const string Group3Name = "group3";

		private static readonly string[] GroupNames = new string[] { Group1Name, Group2Name
			, Group3Name };

		private const int PercentilesInterval = 1;

		private static Configuration conf;

		/// <summary>
		/// UGI should not use the default security conf, else it will collide
		/// with other classes that may change the default conf.
		/// </summary>
		/// <remarks>
		/// UGI should not use the default security conf, else it will collide
		/// with other classes that may change the default conf.  Using this dummy
		/// class that simply throws an exception will ensure that the tests fail
		/// if UGI uses the static default config instead of its own config
		/// </remarks>
		private class DummyLoginConfiguration : Configuration
		{
			// Rollover interval of percentile metrics (in seconds)
			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				throw new RuntimeException("UGI is not using its own security conf!");
			}
		}

		/// <summary>configure ugi</summary>
		[BeforeClass]
		public static void Setup()
		{
			Configuration.SetConfiguration(new TestUserGroupInformation.DummyLoginConfiguration
				());
			// doesn't matter what it is, but getGroups needs it set...
			// use HADOOP_HOME environment variable to prevent interfering with logic
			// that finds winutils.exe
			string home = Runtime.Getenv("HADOOP_HOME");
			Runtime.SetProperty("hadoop.home.dir", (home != null ? home : "."));
			// fake the realm is kerberos is enabled
			Runtime.SetProperty("java.security.krb5.kdc", string.Empty);
			Runtime.SetProperty("java.security.krb5.realm", "DEFAULT.REALM");
		}

		[SetUp]
		public virtual void SetupUgi()
		{
			conf = new Configuration();
			UserGroupInformation.Reset();
			UserGroupInformation.SetConfiguration(conf);
		}

		[TearDown]
		public virtual void ResetUgi()
		{
			UserGroupInformation.SetLoginUser(null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSimpleLogin()
		{
			TryLoginAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Simple, true
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTokenLogin()
		{
			TryLoginAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Token, false
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestProxyLogin()
		{
			TryLoginAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Proxy, false
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TryLoginAuthenticationMethod(UserGroupInformation.AuthenticationMethod
			 method, bool expectSuccess)
		{
			SecurityUtil.SetAuthenticationMethod(method, conf);
			UserGroupInformation.SetConfiguration(conf);
			// pick up changed auth       
			UserGroupInformation ugi = null;
			Exception ex = null;
			try
			{
				ugi = UserGroupInformation.GetLoginUser();
			}
			catch (Exception e)
			{
				ex = e;
			}
			if (expectSuccess)
			{
				NUnit.Framework.Assert.IsNotNull(ugi);
				Assert.Equal(method, ugi.GetAuthenticationMethod());
			}
			else
			{
				NUnit.Framework.Assert.IsNotNull(ex);
				Assert.Equal(typeof(NotSupportedException), ex.GetType());
				Assert.Equal(method + " login authentication is not supported"
					, ex.Message);
			}
		}

		public virtual void TestGetRealAuthenticationMethod()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("user1");
			ugi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Simple);
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Simple, 
				ugi.GetAuthenticationMethod());
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Simple, 
				ugi.GetRealAuthenticationMethod());
			ugi = UserGroupInformation.CreateProxyUser("user2", ugi);
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Proxy, 
				ugi.GetAuthenticationMethod());
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Simple, 
				ugi.GetRealAuthenticationMethod());
		}

		public virtual void TestCreateRemoteUser()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("user1");
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Simple, 
				ugi.GetAuthenticationMethod());
			Assert.True(ugi.ToString().Contains("(auth:SIMPLE)"));
			ugi = UserGroupInformation.CreateRemoteUser("user1", SaslRpcServer.AuthMethod.Kerberos
				);
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Kerberos
				, ugi.GetAuthenticationMethod());
			Assert.True(ugi.ToString().Contains("(auth:KERBEROS)"));
		}

		/// <summary>Test login method</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLogin()
		{
			conf.Set(CommonConfigurationKeys.HadoopUserGroupMetricsPercentilesIntervals, PercentilesInterval
				.ToString());
			UserGroupInformation.SetConfiguration(conf);
			// login from unix
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			Assert.Equal(UserGroupInformation.GetCurrentUser(), UserGroupInformation
				.GetLoginUser());
			Assert.True(ugi.GetGroupNames().Length >= 1);
			VerifyGroupMetrics(1);
			// ensure that doAs works correctly
			UserGroupInformation userGroupInfo = UserGroupInformation.CreateUserForTesting(UserName
				, GroupNames);
			UserGroupInformation curUGI = userGroupInfo.DoAs(new _PrivilegedExceptionAction_185
				());
			// make sure in the scope of the doAs, the right user is current
			Assert.Equal(curUGI, userGroupInfo);
			// make sure it is not the same as the login user
			NUnit.Framework.Assert.IsFalse(curUGI.Equals(UserGroupInformation.GetLoginUser())
				);
		}

		private sealed class _PrivilegedExceptionAction_185 : PrivilegedExceptionAction<UserGroupInformation
			>
		{
			public _PrivilegedExceptionAction_185()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public UserGroupInformation Run()
			{
				return UserGroupInformation.GetCurrentUser();
			}
		}

		/// <summary>given user name - get all the groups.</summary>
		/// <remarks>
		/// given user name - get all the groups.
		/// Needs to happen before creating the test users
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetServerSideGroups()
		{
			// get the user name
			SystemProcess pp = Runtime.GetRuntime().Exec("whoami");
			BufferedReader br = new BufferedReader(new InputStreamReader(pp.GetInputStream())
				);
			string userName = br.ReadLine().Trim();
			// If on windows domain, token format is DOMAIN\\user and we want to
			// extract only the user name
			if (Shell.Windows)
			{
				int sp = userName.LastIndexOf('\\');
				if (sp != -1)
				{
					userName = Runtime.Substring(userName, sp + 1);
				}
				// user names are case insensitive on Windows. Make consistent
				userName = StringUtils.ToLowerCase(userName);
			}
			// get the groups
			pp = Runtime.GetRuntime().Exec(Shell.Windows ? Shell.Winutils + " groups -F" : "id -Gn"
				);
			br = new BufferedReader(new InputStreamReader(pp.GetInputStream()));
			string line = br.ReadLine();
			System.Console.Out.WriteLine(userName + ":" + line);
			ICollection<string> groups = new LinkedHashSet<string>();
			string[] tokens = line.Split(Shell.TokenSeparatorRegex);
			foreach (string s in tokens)
			{
				groups.AddItem(s);
			}
			UserGroupInformation login = UserGroupInformation.GetCurrentUser();
			string loginUserName = login.GetShortUserName();
			if (Shell.Windows)
			{
				// user names are case insensitive on Windows. Make consistent
				loginUserName = StringUtils.ToLowerCase(loginUserName);
			}
			Assert.Equal(userName, loginUserName);
			string[] gi = login.GetGroupNames();
			Assert.Equal(groups.Count, gi.Length);
			for (int i = 0; i < gi.Length; i++)
			{
				Assert.True(groups.Contains(gi[i]));
			}
			UserGroupInformation fakeUser = UserGroupInformation.CreateRemoteUser("foo.bar");
			fakeUser.DoAs(new _PrivilegedExceptionAction_248(login, fakeUser));
		}

		private sealed class _PrivilegedExceptionAction_248 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_248(UserGroupInformation login, UserGroupInformation
				 fakeUser)
			{
				this.login = login;
				this.fakeUser = fakeUser;
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				UserGroupInformation current = UserGroupInformation.GetCurrentUser();
				NUnit.Framework.Assert.IsFalse(current.Equals(login));
				Assert.Equal(current, fakeUser);
				Assert.Equal(0, current.GetGroupNames().Length);
				return null;
			}

			private readonly UserGroupInformation login;

			private readonly UserGroupInformation fakeUser;
		}

		/// <summary>test constructor</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConstructor()
		{
			// security off, so default should just return simple name
			TestConstructorSuccess("user1", "user1");
			TestConstructorSuccess("user2@DEFAULT.REALM", "user2");
			TestConstructorSuccess("user3/cron@DEFAULT.REALM", "user3");
			TestConstructorSuccess("user4@OTHER.REALM", "user4");
			TestConstructorSuccess("user5/cron@OTHER.REALM", "user5");
			// failure test
			TestConstructorFailures(null);
			TestConstructorFailures(string.Empty);
		}

		/// <summary>test constructor</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConstructorWithRules()
		{
			// security off, but use rules if explicitly set
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[1:$1@$0](.*@OTHER.REALM)s/(.*)@.*/other-$1/"
				);
			UserGroupInformation.SetConfiguration(conf);
			TestConstructorSuccess("user1", "user1");
			TestConstructorSuccess("user4@OTHER.REALM", "other-user4");
			// failure test
			TestConstructorFailures("user2@DEFAULT.REALM");
			TestConstructorFailures("user3/cron@DEFAULT.REALM");
			TestConstructorFailures("user5/cron@OTHER.REALM");
			TestConstructorFailures(null);
			TestConstructorFailures(string.Empty);
		}

		/// <summary>test constructor</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConstructorWithKerberos()
		{
			// security on, default is remove default realm
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			UserGroupInformation.SetConfiguration(conf);
			TestConstructorSuccess("user1", "user1");
			TestConstructorSuccess("user2@DEFAULT.REALM", "user2");
			TestConstructorSuccess("user3/cron@DEFAULT.REALM", "user3");
			// failure test
			TestConstructorFailures("user4@OTHER.REALM");
			TestConstructorFailures("user5/cron@OTHER.REALM");
			TestConstructorFailures(null);
			TestConstructorFailures(string.Empty);
		}

		/// <summary>test constructor</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConstructorWithKerberosRules()
		{
			// security on, explicit rules
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[2:$1@$0](.*@OTHER.REALM)s/(.*)@.*/other-$1/"
				 + "RULE:[1:$1@$0](.*@OTHER.REALM)s/(.*)@.*/other-$1/" + "DEFAULT");
			UserGroupInformation.SetConfiguration(conf);
			TestConstructorSuccess("user1", "user1");
			TestConstructorSuccess("user2@DEFAULT.REALM", "user2");
			TestConstructorSuccess("user3/cron@DEFAULT.REALM", "user3");
			TestConstructorSuccess("user4@OTHER.REALM", "other-user4");
			TestConstructorSuccess("user5/cron@OTHER.REALM", "other-user5");
			// failure test
			TestConstructorFailures(null);
			TestConstructorFailures(string.Empty);
		}

		private void TestConstructorSuccess(string principal, string shortName)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(principal, GroupNames
				);
			// make sure the short and full user names are correct
			Assert.Equal(principal, ugi.GetUserName());
			Assert.Equal(shortName, ugi.GetShortUserName());
		}

		private void TestConstructorFailures(string userName)
		{
			try
			{
				UserGroupInformation.CreateRemoteUser(userName);
				NUnit.Framework.Assert.Fail("user:" + userName + " wasn't invalid");
			}
			catch (ArgumentException e)
			{
				string expect = (userName == null || userName.IsEmpty()) ? "Null user" : "Illegal principal name "
					 + userName;
				Assert.True("Did not find " + expect + " in " + e, e.ToString()
					.Contains(expect));
			}
		}

		public virtual void TestSetConfigWithRules()
		{
			string[] rules = new string[] { "RULE:[1:TEST1]", "RULE:[1:TEST2]", "RULE:[1:TEST3]"
				 };
			// explicitly set a rule
			UserGroupInformation.Reset();
			NUnit.Framework.Assert.IsFalse(KerberosName.HasRulesBeenSet());
			KerberosName.SetRules(rules[0]);
			Assert.True(KerberosName.HasRulesBeenSet());
			Assert.Equal(rules[0], KerberosName.GetRules());
			// implicit init should honor rules already being set
			UserGroupInformation.CreateUserForTesting("someone", new string[0]);
			Assert.Equal(rules[0], KerberosName.GetRules());
			// set conf, should override
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, rules[1]);
			UserGroupInformation.SetConfiguration(conf);
			Assert.Equal(rules[1], KerberosName.GetRules());
			// set conf, should again override
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, rules[2]);
			UserGroupInformation.SetConfiguration(conf);
			Assert.Equal(rules[2], KerberosName.GetRules());
			// implicit init should honor rules already being set
			UserGroupInformation.CreateUserForTesting("someone", new string[0]);
			Assert.Equal(rules[2], KerberosName.GetRules());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEnsureInitWithRules()
		{
			string rules = "RULE:[1:RULE1]";
			// trigger implicit init, rules should init
			UserGroupInformation.Reset();
			NUnit.Framework.Assert.IsFalse(KerberosName.HasRulesBeenSet());
			UserGroupInformation.CreateUserForTesting("someone", new string[0]);
			Assert.True(KerberosName.HasRulesBeenSet());
			// set a rule, trigger implicit init, rule should not change 
			UserGroupInformation.Reset();
			KerberosName.SetRules(rules);
			Assert.True(KerberosName.HasRulesBeenSet());
			Assert.Equal(rules, KerberosName.GetRules());
			UserGroupInformation.CreateUserForTesting("someone", new string[0]);
			Assert.Equal(rules, KerberosName.GetRules());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEquals()
		{
			UserGroupInformation uugi = UserGroupInformation.CreateUserForTesting(UserName, GroupNames
				);
			Assert.Equal(uugi, uugi);
			// The subjects should be different, so this should fail
			UserGroupInformation ugi2 = UserGroupInformation.CreateUserForTesting(UserName, GroupNames
				);
			NUnit.Framework.Assert.IsFalse(uugi.Equals(ugi2));
			NUnit.Framework.Assert.IsFalse(uugi.GetHashCode() == ugi2.GetHashCode());
			// two ugi that have the same subject need to be equal
			UserGroupInformation ugi3 = new UserGroupInformation(uugi.GetSubject());
			Assert.Equal(uugi, ugi3);
			Assert.Equal(uugi.GetHashCode(), ugi3.GetHashCode());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEqualsWithRealUser()
		{
			UserGroupInformation realUgi1 = UserGroupInformation.CreateUserForTesting("RealUser"
				, GroupNames);
			UserGroupInformation proxyUgi1 = UserGroupInformation.CreateProxyUser(UserName, realUgi1
				);
			UserGroupInformation proxyUgi2 = new UserGroupInformation(proxyUgi1.GetSubject());
			UserGroupInformation remoteUgi = UserGroupInformation.CreateRemoteUser(UserName);
			Assert.Equal(proxyUgi1, proxyUgi2);
			NUnit.Framework.Assert.IsFalse(remoteUgi.Equals(proxyUgi1));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGettingGroups()
		{
			UserGroupInformation uugi = UserGroupInformation.CreateUserForTesting(UserName, GroupNames
				);
			Assert.Equal(UserName, uugi.GetUserName());
			Assert.AssertArrayEquals(new string[] { Group1Name, Group2Name, Group3Name }, uugi
				.GetGroupNames());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddToken<T>()
			where T : TokenIdentifier
		{
			// from Mockito mocks
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("someone");
			Org.Apache.Hadoop.Security.Token.Token<T> t1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Apache.Hadoop.Security.Token.Token<T> t2 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Apache.Hadoop.Security.Token.Token<T> t3 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			// add token to ugi
			ugi.AddToken(t1);
			CheckTokens(ugi, t1);
			// replace token t1 with t2 - with same key (null)
			ugi.AddToken(t2);
			CheckTokens(ugi, t2);
			// change t1 service and add token
			Org.Mockito.Mockito.When(t1.GetService()).ThenReturn(new Text("t1"));
			ugi.AddToken(t1);
			CheckTokens(ugi, t1, t2);
			// overwrite t1 token with t3 - same key (!null)
			Org.Mockito.Mockito.When(t3.GetService()).ThenReturn(new Text("t1"));
			ugi.AddToken(t3);
			CheckTokens(ugi, t2, t3);
			// just try to re-add with new name
			Org.Mockito.Mockito.When(t1.GetService()).ThenReturn(new Text("t1.1"));
			ugi.AddToken(t1);
			CheckTokens(ugi, t1, t2, t3);
			// just try to re-add with new name again
			ugi.AddToken(t1);
			CheckTokens(ugi, t1, t2, t3);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetCreds<T>()
			where T : TokenIdentifier
		{
			// from Mockito mocks
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("someone");
			Text service = new Text("service");
			Org.Apache.Hadoop.Security.Token.Token<T> t1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t1.GetService()).ThenReturn(service);
			Org.Apache.Hadoop.Security.Token.Token<T> t2 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t2.GetService()).ThenReturn(new Text("service2"));
			Org.Apache.Hadoop.Security.Token.Token<T> t3 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t3.GetService()).ThenReturn(service);
			// add token to ugi
			ugi.AddToken(t1);
			ugi.AddToken(t2);
			CheckTokens(ugi, t1, t2);
			Credentials creds = ugi.GetCredentials();
			creds.AddToken(t3.GetService(), t3);
			NUnit.Framework.Assert.AreSame(t3, creds.GetToken(service));
			// check that ugi wasn't modified
			CheckTokens(ugi, t1, t2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddCreds<T>()
			where T : TokenIdentifier
		{
			// from Mockito mocks
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("someone");
			Text service = new Text("service");
			Org.Apache.Hadoop.Security.Token.Token<T> t1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t1.GetService()).ThenReturn(service);
			Org.Apache.Hadoop.Security.Token.Token<T> t2 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t2.GetService()).ThenReturn(new Text("service2"));
			byte[] secret = new byte[] {  };
			Text secretKey = new Text("sshhh");
			// fill credentials
			Credentials creds = new Credentials();
			creds.AddToken(t1.GetService(), t1);
			creds.AddToken(t2.GetService(), t2);
			creds.AddSecretKey(secretKey, secret);
			// add creds to ugi, and check ugi
			ugi.AddCredentials(creds);
			CheckTokens(ugi, t1, t2);
			NUnit.Framework.Assert.AreSame(secret, ugi.GetCredentials().GetSecretKey(secretKey
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetCredsNotSame<T>()
			where T : TokenIdentifier
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("someone");
			Credentials creds = ugi.GetCredentials();
			// should always get a new copy
			NUnit.Framework.Assert.AreNotSame(creds, ugi.GetCredentials());
		}

		private void CheckTokens(UserGroupInformation ugi, params Org.Apache.Hadoop.Security.Token.Token
			<object>[] tokens)
		{
			// check the ugi's token collection
			ICollection<Org.Apache.Hadoop.Security.Token.Token<object>> ugiTokens = ugi.GetTokens
				();
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> t in tokens)
			{
				Assert.True(ugiTokens.Contains(t));
			}
			Assert.Equal(tokens.Length, ugiTokens.Count);
			// check the ugi's credentials
			Credentials ugiCreds = ugi.GetCredentials();
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> t_1 in tokens)
			{
				NUnit.Framework.Assert.AreSame(t_1, ugiCreds.GetToken(t_1.GetService()));
			}
			Assert.Equal(tokens.Length, ugiCreds.NumberOfTokens());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddNamedToken<T>()
			where T : TokenIdentifier
		{
			// from Mockito mocks
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("someone");
			Org.Apache.Hadoop.Security.Token.Token<T> t1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Text service1 = new Text("t1");
			Text service2 = new Text("t2");
			Org.Mockito.Mockito.When(t1.GetService()).ThenReturn(service1);
			// add token
			ugi.AddToken(service1, t1);
			NUnit.Framework.Assert.AreSame(t1, ugi.GetCredentials().GetToken(service1));
			// add token with another name
			ugi.AddToken(service2, t1);
			NUnit.Framework.Assert.AreSame(t1, ugi.GetCredentials().GetToken(service1));
			NUnit.Framework.Assert.AreSame(t1, ugi.GetCredentials().GetToken(service2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUGITokens<T>()
			where T : TokenIdentifier
		{
			// from Mockito mocks
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("TheDoctor", 
				new string[] { "TheTARDIS" });
			Org.Apache.Hadoop.Security.Token.Token<T> t1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t1.GetService()).ThenReturn(new Text("t1"));
			Org.Apache.Hadoop.Security.Token.Token<T> t2 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.When(t2.GetService()).ThenReturn(new Text("t2"));
			Credentials creds = new Credentials();
			byte[] secretKey = new byte[] {  };
			Text secretName = new Text("shhh");
			creds.AddSecretKey(secretName, secretKey);
			ugi.AddToken(t1);
			ugi.AddToken(t2);
			ugi.AddCredentials(creds);
			ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> z = ugi.GetTokens
				();
			Assert.True(z.Contains(t1));
			Assert.True(z.Contains(t2));
			Assert.Equal(2, z.Count);
			Credentials ugiCreds = ugi.GetCredentials();
			NUnit.Framework.Assert.AreSame(secretKey, ugiCreds.GetSecretKey(secretName));
			Assert.Equal(1, ugiCreds.NumberOfSecretKeys());
			try
			{
				z.Remove(t1);
				NUnit.Framework.Assert.Fail("Shouldn't be able to modify token collection from UGI"
					);
			}
			catch (NotSupportedException)
			{
			}
			// Can't modify tokens
			// ensure that the tokens are passed through doAs
			ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> otherSet = ugi
				.DoAs(new _PrivilegedExceptionAction_612());
			Assert.True(otherSet.Contains(t1));
			Assert.True(otherSet.Contains(t2));
		}

		private sealed class _PrivilegedExceptionAction_612 : PrivilegedExceptionAction<ICollection
			<Org.Apache.Hadoop.Security.Token.Token<object>>>
		{
			public _PrivilegedExceptionAction_612()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public ICollection<Org.Apache.Hadoop.Security.Token.Token<object>> Run()
			{
				return UserGroupInformation.GetCurrentUser().GetTokens();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTokenIdentifiers()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("TheDoctor", 
				new string[] { "TheTARDIS" });
			TokenIdentifier t1 = Org.Mockito.Mockito.Mock<TokenIdentifier>();
			TokenIdentifier t2 = Org.Mockito.Mockito.Mock<TokenIdentifier>();
			ugi.AddTokenIdentifier(t1);
			ugi.AddTokenIdentifier(t2);
			ICollection<TokenIdentifier> z = ugi.GetTokenIdentifiers();
			Assert.True(z.Contains(t1));
			Assert.True(z.Contains(t2));
			Assert.Equal(2, z.Count);
			// ensure that the token identifiers are passed through doAs
			ICollection<TokenIdentifier> otherSet = ugi.DoAs(new _PrivilegedExceptionAction_639
				());
			Assert.True(otherSet.Contains(t1));
			Assert.True(otherSet.Contains(t2));
			Assert.Equal(2, otherSet.Count);
		}

		private sealed class _PrivilegedExceptionAction_639 : PrivilegedExceptionAction<ICollection
			<TokenIdentifier>>
		{
			public _PrivilegedExceptionAction_639()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public ICollection<TokenIdentifier> Run()
			{
				return UserGroupInformation.GetCurrentUser().GetTokenIdentifiers();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTestAuthMethod()
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			// verify the reverse mappings works
			foreach (UserGroupInformation.AuthenticationMethod am in UserGroupInformation.AuthenticationMethod
				.Values())
			{
				if (am.GetAuthMethod() != null)
				{
					ugi.SetAuthenticationMethod(am.GetAuthMethod());
					Assert.Equal(am, ugi.GetAuthenticationMethod());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUGIAuthMethod()
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			UserGroupInformation.AuthenticationMethod am = UserGroupInformation.AuthenticationMethod
				.Kerberos;
			ugi.SetAuthenticationMethod(am);
			Assert.Equal(am, ugi.GetAuthenticationMethod());
			ugi.DoAs(new _PrivilegedExceptionAction_668(am));
		}

		private sealed class _PrivilegedExceptionAction_668 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_668(UserGroupInformation.AuthenticationMethod am
				)
			{
				this.am = am;
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				Assert.Equal(am, UserGroupInformation.GetCurrentUser().GetAuthenticationMethod
					());
				return null;
			}

			private readonly UserGroupInformation.AuthenticationMethod am;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUGIAuthMethodInRealUser()
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			UserGroupInformation proxyUgi = UserGroupInformation.CreateProxyUser("proxy", ugi
				);
			UserGroupInformation.AuthenticationMethod am = UserGroupInformation.AuthenticationMethod
				.Kerberos;
			ugi.SetAuthenticationMethod(am);
			Assert.Equal(am, ugi.GetAuthenticationMethod());
			Assert.Equal(UserGroupInformation.AuthenticationMethod.Proxy, 
				proxyUgi.GetAuthenticationMethod());
			Assert.Equal(am, UserGroupInformation.GetRealAuthenticationMethod
				(proxyUgi));
			proxyUgi.DoAs(new _PrivilegedExceptionAction_690(am));
			UserGroupInformation proxyUgi2 = new UserGroupInformation(proxyUgi.GetSubject());
			proxyUgi2.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Proxy
				);
			Assert.Equal(proxyUgi, proxyUgi2);
			// Equality should work if authMethod is null
			UserGroupInformation realugi = UserGroupInformation.GetCurrentUser();
			UserGroupInformation proxyUgi3 = UserGroupInformation.CreateProxyUser("proxyAnother"
				, realugi);
			UserGroupInformation proxyUgi4 = new UserGroupInformation(proxyUgi3.GetSubject());
			Assert.Equal(proxyUgi3, proxyUgi4);
		}

		private sealed class _PrivilegedExceptionAction_690 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_690(UserGroupInformation.AuthenticationMethod am
				)
			{
				this.am = am;
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				Assert.Equal(UserGroupInformation.AuthenticationMethod.Proxy, 
					UserGroupInformation.GetCurrentUser().GetAuthenticationMethod());
				Assert.Equal(am, UserGroupInformation.GetCurrentUser().GetRealUser
					().GetAuthenticationMethod());
				return null;
			}

			private readonly UserGroupInformation.AuthenticationMethod am;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLoginObjectInSubject()
		{
			UserGroupInformation loginUgi = UserGroupInformation.GetLoginUser();
			UserGroupInformation anotherUgi = new UserGroupInformation(loginUgi.GetSubject());
			LoginContext login1 = loginUgi.GetSubject().GetPrincipals<User>().GetEnumerator()
				.Next().GetLogin();
			LoginContext login2 = anotherUgi.GetSubject().GetPrincipals<User>().GetEnumerator
				().Next().GetLogin();
			//login1 and login2 must be same instances
			Assert.True(login1 == login2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLoginModuleCommit()
		{
			UserGroupInformation loginUgi = UserGroupInformation.GetLoginUser();
			User user1 = loginUgi.GetSubject().GetPrincipals<User>().GetEnumerator().Next();
			LoginContext login = user1.GetLogin();
			login.Logout();
			login.Login();
			User user2 = loginUgi.GetSubject().GetPrincipals<User>().GetEnumerator().Next();
			// user1 and user2 must be same instances.
			Assert.True(user1 == user2);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void VerifyLoginMetrics(long success, int failure)
		{
			// Ensure metrics related to kerberos login is updated.
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics("UgiMetrics");
			if (success > 0)
			{
				MetricsAsserts.AssertCounter("LoginSuccessNumOps", success, rb);
				MetricsAsserts.AssertGaugeGt("LoginSuccessAvgTime", 0, rb);
			}
			if (failure > 0)
			{
				MetricsAsserts.AssertCounter("LoginFailureNumPos", failure, rb);
				MetricsAsserts.AssertGaugeGt("LoginFailureAvgTime", 0, rb);
			}
		}

		/// <exception cref="System.Exception"/>
		private static void VerifyGroupMetrics(long groups)
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics("UgiMetrics");
			if (groups > 0)
			{
				MetricsAsserts.AssertCounterGt("GetGroupsNumOps", groups - 1, rb);
				double avg = MetricsAsserts.GetDoubleGauge("GetGroupsAvgTime", rb);
				Assert.True(avg >= 0.0);
				// Sleep for an interval+slop to let the percentiles rollover
				Thread.Sleep((PercentilesInterval + 1) * 1000);
				// Check that the percentiles were updated
				MetricsAsserts.AssertQuantileGauges("GetGroups1s", rb);
			}
		}

		/// <summary>
		/// Test for the case that UserGroupInformation.getCurrentUser()
		/// is called when the AccessControlContext has a Subject associated
		/// with it, but that Subject was not created by Hadoop (ie it has no
		/// associated User principal)
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUGIUnderNonHadoopContext()
		{
			Subject nonHadoopSubject = new Subject();
			Subject.DoAs(nonHadoopSubject, new _PrivilegedExceptionAction_778());
		}

		private sealed class _PrivilegedExceptionAction_778 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_778()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				NUnit.Framework.Assert.IsNotNull(ugi);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetUGIFromSubject()
		{
			KerberosPrincipal p = new KerberosPrincipal("guest");
			Subject subject = new Subject();
			subject.GetPrincipals().AddItem(p);
			UserGroupInformation ugi = UserGroupInformation.GetUGIFromSubject(subject);
			NUnit.Framework.Assert.IsNotNull(ugi);
			Assert.Equal("guest@DEFAULT.REALM", ugi.GetUserName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetLoginUser()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test-user");
			UserGroupInformation.SetLoginUser(ugi);
			Assert.Equal(ugi, UserGroupInformation.GetLoginUser());
		}

		/// <summary>
		/// In some scenario, such as HA, delegation tokens are associated with a
		/// logical name.
		/// </summary>
		/// <remarks>
		/// In some scenario, such as HA, delegation tokens are associated with a
		/// logical name. The tokens are cloned and are associated with the
		/// physical address of the server where the service is provided.
		/// This test ensures cloned delegated tokens are locally used
		/// and are not returned in
		/// <see cref="UserGroupInformation.GetCredentials()"/>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPrivateTokenExclusion()
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			TestSaslRPC.TestTokenIdentifier tokenId = new TestSaslRPC.TestTokenIdentifier();
			Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier> token = new 
				Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier>(tokenId.
				GetBytes(), Runtime.GetBytesForString("password"), tokenId.GetKind(), null
				);
			ugi.AddToken(new Text("regular-token"), token);
			// Now add cloned private token
			ugi.AddToken(new Text("private-token"), new Token.PrivateToken<TestSaslRPC.TestTokenIdentifier
				>(token));
			ugi.AddToken(new Text("private-token1"), new Token.PrivateToken<TestSaslRPC.TestTokenIdentifier
				>(token));
			// Ensure only non-private tokens are returned
			ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> tokens = ugi
				.GetCredentials().GetAllTokens();
			Assert.Equal(1, tokens.Count);
		}

		/// <summary>
		/// This test checks a race condition between getting and adding tokens for
		/// the current user.
		/// </summary>
		/// <remarks>
		/// This test checks a race condition between getting and adding tokens for
		/// the current user.  Calling UserGroupInformation.getCurrentUser() returns
		/// a new object each time, so simply making these methods synchronized was not
		/// enough to prevent race conditions and causing a
		/// ConcurrentModificationException.  These methods are synchronized on the
		/// Subject, which is the same object between UserGroupInformation instances.
		/// This test tries to cause a CME, by exposing the race condition.  Previously
		/// this test would fail every time; now it does not.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTokenRaceCondition()
		{
			UserGroupInformation userGroupInfo = UserGroupInformation.CreateUserForTesting(UserName
				, GroupNames);
			userGroupInfo.DoAs(new _PrivilegedExceptionAction_844());
		}

		private sealed class _PrivilegedExceptionAction_844 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_844()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				// make sure it is not the same as the login user because we use the
				// same UGI object for every instantiation of the login user and you
				// won't run into the race condition otherwise
				Assert.AssertNotEquals(UserGroupInformation.GetLoginUser(), UserGroupInformation.
					GetCurrentUser());
				TestUserGroupInformation.GetTokenThread thread = new TestUserGroupInformation.GetTokenThread
					();
				try
				{
					thread.Start();
					for (int i = 0; i < 100; i++)
					{
						Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t = Org.Mockito.Mockito.Mock
							<Org.Apache.Hadoop.Security.Token.Token>();
						Org.Mockito.Mockito.When(t.GetService()).ThenReturn(new Text("t" + i));
						UserGroupInformation.GetCurrentUser().AddToken(t);
						NUnit.Framework.Assert.IsNull("ConcurrentModificationException encountered", thread
							.cme);
					}
				}
				catch (ConcurrentModificationException cme)
				{
					Runtime.PrintStackTrace(cme);
					NUnit.Framework.Assert.Fail("ConcurrentModificationException encountered");
				}
				finally
				{
					thread.runThread = false;
					thread.Join(5 * 1000);
				}
				return null;
			}
		}

		internal class GetTokenThread : Thread
		{
			internal bool runThread = true;

			internal volatile ConcurrentModificationException cme = null;

			public override void Run()
			{
				while (runThread)
				{
					try
					{
						UserGroupInformation.GetCurrentUser().GetCredentials();
					}
					catch (ConcurrentModificationException cme)
					{
						this.cme = cme;
						Runtime.PrintStackTrace(cme);
						runThread = false;
					}
					catch (IOException ex)
					{
						Runtime.PrintStackTrace(ex);
					}
				}
			}
		}
	}
}
