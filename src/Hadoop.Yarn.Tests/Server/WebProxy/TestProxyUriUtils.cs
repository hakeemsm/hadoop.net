using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	public class TestProxyUriUtils
	{
		[NUnit.Framework.Test]
		public virtual void TestGetPathApplicationId()
		{
			NUnit.Framework.Assert.AreEqual("/proxy/application_100_0001", ProxyUriUtils.GetPath
				(BuilderUtils.NewApplicationId(100l, 1)));
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005", ProxyUriUtils.
				GetPath(BuilderUtils.NewApplicationId(6384623l, 5)));
		}

		public virtual void TestGetPathApplicationIdBad()
		{
			ProxyUriUtils.GetPath(null);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetPathApplicationIdString()
		{
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005", ProxyUriUtils.
				GetPath(BuilderUtils.NewApplicationId(6384623l, 5), null));
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005/static/app", ProxyUriUtils
				.GetPath(BuilderUtils.NewApplicationId(6384623l, 5), "/static/app"));
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005/", ProxyUriUtils
				.GetPath(BuilderUtils.NewApplicationId(6384623l, 5), "/"));
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005/some/path", ProxyUriUtils
				.GetPath(BuilderUtils.NewApplicationId(6384623l, 5), "some/path"));
		}

		[NUnit.Framework.Test]
		public virtual void TestGetPathAndQuery()
		{
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005/static/app?foo=bar"
				, ProxyUriUtils.GetPathAndQuery(BuilderUtils.NewApplicationId(6384623l, 5), "/static/app"
				, "?foo=bar", false));
			NUnit.Framework.Assert.AreEqual("/proxy/application_6384623_0005/static/app?foo=bar&bad=good&proxyapproved=true"
				, ProxyUriUtils.GetPathAndQuery(BuilderUtils.NewApplicationId(6384623l, 5), "/static/app"
				, "foo=bar&bad=good", true));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetProxyUri()
		{
			URI originalUri = new URI("http://host.com/static/foo?bar=bar");
			URI proxyUri = new URI("http://proxy.net:8080/");
			ApplicationId id = BuilderUtils.NewApplicationId(6384623l, 5);
			URI expected = new URI("http://proxy.net:8080/proxy/application_6384623_0005/static/foo?bar=bar"
				);
			URI result = ProxyUriUtils.GetProxyUri(originalUri, proxyUri, id);
			NUnit.Framework.Assert.AreEqual(expected, result);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetProxyUriNull()
		{
			URI originalUri = null;
			URI proxyUri = new URI("http://proxy.net:8080/");
			ApplicationId id = BuilderUtils.NewApplicationId(6384623l, 5);
			URI expected = new URI("http://proxy.net:8080/proxy/application_6384623_0005/");
			URI result = ProxyUriUtils.GetProxyUri(originalUri, proxyUri, id);
			NUnit.Framework.Assert.AreEqual(expected, result);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetProxyUriFromPluginsReturnsNullIfNoPlugins()
		{
			ApplicationId id = BuilderUtils.NewApplicationId(6384623l, 5);
			IList<TrackingUriPlugin> list = Lists.NewArrayListWithExpectedSize(0);
			NUnit.Framework.Assert.IsNull(ProxyUriUtils.GetUriFromTrackingPlugins(id, list));
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetProxyUriFromPluginsReturnsValidUriWhenAble()
		{
			ApplicationId id = BuilderUtils.NewApplicationId(6384623l, 5);
			IList<TrackingUriPlugin> list = Lists.NewArrayListWithExpectedSize(2);
			// Insert a plugin that returns null.
			list.AddItem(new _TrackingUriPlugin_108());
			// Insert a plugin that returns a valid URI.
			list.AddItem(new _TrackingUriPlugin_114());
			URI result = ProxyUriUtils.GetUriFromTrackingPlugins(id, list);
			NUnit.Framework.Assert.IsNotNull(result);
		}

		private sealed class _TrackingUriPlugin_108 : TrackingUriPlugin
		{
			public _TrackingUriPlugin_108()
			{
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			public override URI GetTrackingUri(ApplicationId id)
			{
				return null;
			}
		}

		private sealed class _TrackingUriPlugin_114 : TrackingUriPlugin
		{
			public _TrackingUriPlugin_114()
			{
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			public override URI GetTrackingUri(ApplicationId id)
			{
				return new URI("http://history.server.net/");
			}
		}
	}
}
