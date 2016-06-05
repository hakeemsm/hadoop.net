using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestLocalResource
	{
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal static LocalResource GetYarnResource(Path p, long size, long timestamp, 
			LocalResourceType type, LocalResourceVisibility state, string pattern)
		{
			LocalResource ret = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance
				<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromURI(p.ToUri()));
			ret.SetSize(size);
			ret.SetTimestamp(timestamp);
			ret.SetType(type);
			ret.SetVisibility(state);
			ret.SetPattern(pattern);
			return ret;
		}

		internal static void CheckEqual(LocalResourceRequest a, LocalResourceRequest b)
		{
			NUnit.Framework.Assert.AreEqual(a, b);
			NUnit.Framework.Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
			NUnit.Framework.Assert.AreEqual(0, a.CompareTo(b));
			NUnit.Framework.Assert.AreEqual(0, b.CompareTo(a));
		}

		internal static void CheckNotEqual(LocalResourceRequest a, LocalResourceRequest b
			)
		{
			NUnit.Framework.Assert.IsFalse(a.Equals(b));
			NUnit.Framework.Assert.IsFalse(b.Equals(a));
			NUnit.Framework.Assert.IsFalse(a.GetHashCode() == b.GetHashCode());
			NUnit.Framework.Assert.IsFalse(0 == a.CompareTo(b));
			NUnit.Framework.Assert.IsFalse(0 == b.CompareTo(a));
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceEquality()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			long basetime = (long)(((ulong)r.NextLong()) >> 2);
			LocalResource yA = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime
				, LocalResourceType.File, LocalResourceVisibility.Public, null);
			LocalResource yB = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime
				, LocalResourceType.File, LocalResourceVisibility.Public, null);
			LocalResourceRequest a = new LocalResourceRequest(yA);
			LocalResourceRequest b = new LocalResourceRequest(yA);
			CheckEqual(a, b);
			b = new LocalResourceRequest(yB);
			CheckEqual(a, b);
			// ignore visibility
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime, LocalResourceType
				.File, LocalResourceVisibility.Private, null);
			b = new LocalResourceRequest(yB);
			CheckEqual(a, b);
			// ignore size
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), 0, basetime, LocalResourceType
				.File, LocalResourceVisibility.Private, null);
			b = new LocalResourceRequest(yB);
			CheckEqual(a, b);
			// note path
			yB = GetYarnResource(new Path("hdfs://dingo.org:80/foobar"), 0, basetime, LocalResourceType
				.Archive, LocalResourceVisibility.Public, null);
			b = new LocalResourceRequest(yB);
			CheckNotEqual(a, b);
			// note type
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), 0, basetime, LocalResourceType
				.Archive, LocalResourceVisibility.Public, null);
			b = new LocalResourceRequest(yB);
			CheckNotEqual(a, b);
			// note timestamp
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), 0, basetime + 1, LocalResourceType
				.File, LocalResourceVisibility.Public, null);
			b = new LocalResourceRequest(yB);
			CheckNotEqual(a, b);
			// note pattern
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), 0, basetime + 1, LocalResourceType
				.File, LocalResourceVisibility.Public, "^/foo/.*");
			b = new LocalResourceRequest(yB);
			CheckNotEqual(a, b);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceOrder()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			long basetime = (long)(((ulong)r.NextLong()) >> 2);
			LocalResource yA = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime
				, LocalResourceType.File, LocalResourceVisibility.Public, "^/foo/.*");
			LocalResourceRequest a = new LocalResourceRequest(yA);
			// Path primary
			LocalResource yB = GetYarnResource(new Path("http://yak.org:80/foobaz"), -1, basetime
				, LocalResourceType.File, LocalResourceVisibility.Public, "^/foo/.*");
			LocalResourceRequest b = new LocalResourceRequest(yB);
			NUnit.Framework.Assert.IsTrue(0 > a.CompareTo(b));
			// timestamp secondary
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime + 1, LocalResourceType
				.File, LocalResourceVisibility.Public, "^/foo/.*");
			b = new LocalResourceRequest(yB);
			NUnit.Framework.Assert.IsTrue(0 > a.CompareTo(b));
			// type tertiary
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime, LocalResourceType
				.Archive, LocalResourceVisibility.Public, "^/foo/.*");
			b = new LocalResourceRequest(yB);
			NUnit.Framework.Assert.IsTrue(0 != a.CompareTo(b));
			// don't care about order, just ne
			// path 4th
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime, LocalResourceType
				.Archive, LocalResourceVisibility.Public, "^/food/.*");
			b = new LocalResourceRequest(yB);
			NUnit.Framework.Assert.IsTrue(0 != a.CompareTo(b));
			// don't care about order, just ne
			yB = GetYarnResource(new Path("http://yak.org:80/foobar"), -1, basetime, LocalResourceType
				.Archive, LocalResourceVisibility.Public, null);
			b = new LocalResourceRequest(yB);
			NUnit.Framework.Assert.IsTrue(0 != a.CompareTo(b));
		}
		// don't care about order, just ne
	}
}
