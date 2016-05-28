using System.Collections.Generic;
using Javax.Servlet;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestAuthFilter
	{
		private class DummyFilterConfig : FilterConfig
		{
			internal readonly IDictionary<string, string> map;

			internal DummyFilterConfig(IDictionary<string, string> map)
			{
				this.map = map;
			}

			public virtual string GetFilterName()
			{
				return "dummy";
			}

			public virtual string GetInitParameter(string arg0)
			{
				return map[arg0];
			}

			public virtual Enumeration<string> GetInitParameterNames()
			{
				return Sharpen.Collections.Enumeration(map.Keys);
			}

			public virtual ServletContext GetServletContext()
			{
				return null;
			}
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetConfiguration()
		{
			AuthFilter filter = new AuthFilter();
			IDictionary<string, string> m = new Dictionary<string, string>();
			m[DFSConfigKeys.DfsWebAuthenticationKerberosPrincipalKey] = "xyz/thehost@REALM";
			m[DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey] = "thekeytab";
			FilterConfig config = new TestAuthFilter.DummyFilterConfig(m);
			Properties p = filter.GetConfiguration("random", config);
			NUnit.Framework.Assert.AreEqual("xyz/thehost@REALM", p.GetProperty("kerberos.principal"
				));
			NUnit.Framework.Assert.AreEqual("thekeytab", p.GetProperty("kerberos.keytab"));
			NUnit.Framework.Assert.AreEqual("true", p.GetProperty(PseudoAuthenticationHandler
				.AnonymousAllowed));
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSimpleAuthDisabledConfiguration()
		{
			AuthFilter filter = new AuthFilter();
			IDictionary<string, string> m = new Dictionary<string, string>();
			m[DFSConfigKeys.DfsWebAuthenticationSimpleAnonymousAllowed] = "false";
			FilterConfig config = new TestAuthFilter.DummyFilterConfig(m);
			Properties p = filter.GetConfiguration("random", config);
			NUnit.Framework.Assert.AreEqual("false", p.GetProperty(PseudoAuthenticationHandler
				.AnonymousAllowed));
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSimpleAuthDefaultConfiguration()
		{
			AuthFilter filter = new AuthFilter();
			IDictionary<string, string> m = new Dictionary<string, string>();
			FilterConfig config = new TestAuthFilter.DummyFilterConfig(m);
			Properties p = filter.GetConfiguration("random", config);
			NUnit.Framework.Assert.AreEqual("true", p.GetProperty(PseudoAuthenticationHandler
				.AnonymousAllowed));
		}
	}
}
