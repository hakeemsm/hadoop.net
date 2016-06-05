using System.Collections.Generic;
using Javax.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class TestSocketFactory
	{
		[Fact]
		public virtual void TestSocketFactoryAsKeyInMap()
		{
			IDictionary<SocketFactory, int> dummyCache = new Dictionary<SocketFactory, int>();
			int toBeCached1 = 1;
			int toBeCached2 = 2;
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopRpcSocketFactoryClassDefaultKey, "org.apache.hadoop.ipc.TestSocketFactory$DummySocketFactory"
				);
			SocketFactory dummySocketFactory = NetUtils.GetDefaultSocketFactory(conf);
			dummyCache[dummySocketFactory] = toBeCached1;
			conf.Set(CommonConfigurationKeys.HadoopRpcSocketFactoryClassDefaultKey, "org.apache.hadoop.net.StandardSocketFactory"
				);
			SocketFactory defaultSocketFactory = NetUtils.GetDefaultSocketFactory(conf);
			dummyCache[defaultSocketFactory] = toBeCached2;
			Assert.Equal("The cache contains two elements", 2, dummyCache.
				Count);
			Assert.Equal("Equals of both socket factory shouldn't be same"
				, defaultSocketFactory.Equals(dummySocketFactory), false);
			NUnit.Framework.Assert.AreSame(toBeCached2, Sharpen.Collections.Remove(dummyCache
				, defaultSocketFactory));
			dummyCache[defaultSocketFactory] = toBeCached2;
			NUnit.Framework.Assert.AreSame(toBeCached1, Sharpen.Collections.Remove(dummyCache
				, dummySocketFactory));
		}

		/// <summary>A dummy socket factory class that extends the StandardSocketFactory.</summary>
		internal class DummySocketFactory : StandardSocketFactory
		{
		}
	}
}
