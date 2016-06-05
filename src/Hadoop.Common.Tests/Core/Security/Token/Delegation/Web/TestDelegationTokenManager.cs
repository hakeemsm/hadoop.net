using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	public class TestDelegationTokenManager
	{
		private const long DayInSecs = 86400;

		[Parameterized.Parameters]
		public static ICollection<object[]> Headers()
		{
			return Arrays.AsList(new object[][] { new object[] { false }, new object[] { true
				 } });
		}

		private bool enableZKKey;

		public TestDelegationTokenManager(bool enableZKKey)
		{
			this.enableZKKey = enableZKKey;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDTManager()
		{
			Configuration conf = new Configuration(false);
			conf.SetLong(DelegationTokenManager.UpdateInterval, DayInSecs);
			conf.SetLong(DelegationTokenManager.MaxLifetime, DayInSecs);
			conf.SetLong(DelegationTokenManager.RenewInterval, DayInSecs);
			conf.SetLong(DelegationTokenManager.RemovalScanInterval, DayInSecs);
			conf.GetBoolean(DelegationTokenManager.EnableZkKey, enableZKKey);
			DelegationTokenManager tm = new DelegationTokenManager(conf, new Text("foo"));
			tm.Init();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)tm.CreateToken(UserGroupInformation.GetCurrentUser()
				, "foo");
			NUnit.Framework.Assert.IsNotNull(token);
			tm.VerifyToken(token);
			Assert.True(tm.RenewToken(token, "foo") > Runtime.CurrentTimeMillis
				());
			tm.CancelToken(token, "foo");
			try
			{
				tm.VerifyToken(token);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				//NOP
				NUnit.Framework.Assert.Fail();
			}
			tm.Destroy();
		}
	}
}
