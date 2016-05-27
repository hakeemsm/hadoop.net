using NUnit.Framework;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs
{
	public class TestNfsExports
	{
		private readonly string address1 = "192.168.0.12";

		private readonly string address2 = "10.0.0.12";

		private readonly string hostname1 = "a.b.com";

		private readonly string hostname2 = "a.b.org";

		private const long ExpirationPeriod = Nfs3Constant.NfsExportsCacheExpirytimeMillisDefault
			 * 1000 * 1000;

		private const int CacheSize = Nfs3Constant.NfsExportsCacheSizeDefault;

		private const long NanosPerMillis = 1000000;

		[NUnit.Framework.Test]
		public virtual void TestWildcardRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "* rw");
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestWildcardRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "* ro");
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestExactAddressRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, address1 + " rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.IsFalse(AccessPrivilege.ReadWrite == matcher.GetAccessPrivilege
				(address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestExactAddressRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, address1);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestExactHostRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, hostname1 + " rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestExactHostRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, hostname1);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestCidrShortRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.0/22 rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestCidrShortRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.0/22"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestCidrLongRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.0/255.255.252.0 rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestCidrLongRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.0/255.255.252.0"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestRegexIPRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.[0-9]+ rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestRegexIPRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.[0-9]+"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, matcher.GetAccessPrivilege(
				address2, hostname1));
		}

		[NUnit.Framework.Test]
		public virtual void TestRegexHostRW()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "[a-z]+.b.com rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname1));
			// address1 will hit the cache
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address1, hostname2));
		}

		[NUnit.Framework.Test]
		public virtual void TestRegexHostRO()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "[a-z]+.b.com");
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			// address1 will hit the cache
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname2));
		}

		[NUnit.Framework.Test]
		public virtual void TestRegexGrouping()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "192.168.0.(12|34)"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			// address1 will hit the cache
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname2));
			matcher = new NfsExports(CacheSize, ExpirationPeriod, "\\w*.a.b.com");
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				("1.2.3.4", "web.a.b.com"));
			// address "1.2.3.4" will hit the cache
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				("1.2.3.4", "email.a.b.org"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiMatchers()
		{
			long shortExpirationPeriod = 1 * 1000 * 1000 * 1000;
			// 1s
			NfsExports matcher = new NfsExports(CacheSize, shortExpirationPeriod, "192.168.0.[0-9]+;[a-z]+.b.com rw"
				);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname2));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, address1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadOnly, matcher.GetAccessPrivilege
				(address1, hostname1));
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address2, hostname1));
			// address2 will hit the cache
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.ReadWrite, matcher.GetAccessPrivilege
				(address2, hostname2));
			Sharpen.Thread.Sleep(1000);
			// no cache for address2 now
			AccessPrivilege ap;
			long startNanos = Runtime.NanoTime();
			do
			{
				ap = matcher.GetAccessPrivilege(address2, address2);
				if (ap == AccessPrivilege.None)
				{
					break;
				}
				Sharpen.Thread.Sleep(500);
			}
			while ((Runtime.NanoTime() - startNanos) / NanosPerMillis < 5000);
			NUnit.Framework.Assert.AreEqual(AccessPrivilege.None, ap);
		}

		public virtual void TestInvalidHost()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "foo#bar");
		}

		public virtual void TestInvalidSeparator()
		{
			NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "foo ro : bar rw"
				);
		}
	}
}
