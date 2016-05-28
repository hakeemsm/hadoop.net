using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public sealed class TestURLConnectionFactory
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public void TestConnConfiguratior()
		{
			Uri u = new Uri("http://localhost");
			IList<HttpURLConnection> conns = Lists.NewArrayList();
			URLConnectionFactory fc = new URLConnectionFactory(new _ConnectionConfigurator_37
				(u, conns));
			fc.OpenConnection(u);
			NUnit.Framework.Assert.AreEqual(1, conns.Count);
		}

		private sealed class _ConnectionConfigurator_37 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_37(Uri u, IList<HttpURLConnection> conns)
			{
				this.u = u;
				this.conns = conns;
			}

			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				NUnit.Framework.Assert.AreEqual(u, conn.GetURL());
				conns.AddItem(conn);
				return conn;
			}

			private readonly Uri u;

			private readonly IList<HttpURLConnection> conns;
		}
	}
}
