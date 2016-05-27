using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestAuthenticationFilter : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestConfiguration()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.http.authentication.foo", "bar");
			conf.Set(HttpServer2.BindAddress, "barhost");
			FilterContainer container = Org.Mockito.Mockito.Mock<FilterContainer>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_45()).When(container).AddFilter(Org.Mockito.Mockito
				.AnyObject<string>(), Org.Mockito.Mockito.AnyObject<string>(), Org.Mockito.Mockito
				.AnyObject<IDictionary<string, string>>());
			new AuthenticationFilterInitializer().InitFilter(container, conf);
		}

		private sealed class _Answer_45 : Answer
		{
			public _Answer_45()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				object[] args = invocationOnMock.GetArguments();
				NUnit.Framework.Assert.AreEqual("authentication", args[0]);
				NUnit.Framework.Assert.AreEqual(typeof(AuthenticationFilter).FullName, args[1]);
				IDictionary<string, string> conf = (IDictionary<string, string>)args[2];
				NUnit.Framework.Assert.AreEqual("/", conf["cookie.path"]);
				NUnit.Framework.Assert.AreEqual("simple", conf["type"]);
				NUnit.Framework.Assert.AreEqual("36000", conf["token.validity"]);
				NUnit.Framework.Assert.IsNull(conf["cookie.domain"]);
				NUnit.Framework.Assert.AreEqual("true", conf["simple.anonymous.allowed"]);
				NUnit.Framework.Assert.AreEqual("HTTP/barhost@LOCALHOST", conf["kerberos.principal"
					]);
				NUnit.Framework.Assert.AreEqual(Runtime.GetProperty("user.home") + "/hadoop.keytab"
					, conf["kerberos.keytab"]);
				NUnit.Framework.Assert.AreEqual("bar", conf["foo"]);
				return null;
			}
		}
	}
}
