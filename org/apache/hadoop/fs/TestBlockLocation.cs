using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestBlockLocation
	{
		private static readonly string[] EMPTY_STR_ARRAY = new string[0];

		/// <exception cref="System.Exception"/>
		private static void checkBlockLocation(org.apache.hadoop.fs.BlockLocation loc)
		{
			checkBlockLocation(loc, 0, 0, false);
		}

		/// <exception cref="System.Exception"/>
		private static void checkBlockLocation(org.apache.hadoop.fs.BlockLocation loc, long
			 offset, long length, bool corrupt)
		{
			checkBlockLocation(loc, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY
				, offset, length, corrupt);
		}

		/// <exception cref="System.Exception"/>
		private static void checkBlockLocation(org.apache.hadoop.fs.BlockLocation loc, string
			[] names, string[] hosts, string[] cachedHosts, string[] topologyPaths, long offset
			, long length, bool corrupt)
		{
			NUnit.Framework.Assert.IsNotNull(loc.getHosts());
			NUnit.Framework.Assert.IsNotNull(loc.getCachedHosts());
			NUnit.Framework.Assert.IsNotNull(loc.getNames());
			NUnit.Framework.Assert.IsNotNull(loc.getTopologyPaths());
			NUnit.Framework.Assert.assertArrayEquals(hosts, loc.getHosts());
			NUnit.Framework.Assert.assertArrayEquals(cachedHosts, loc.getCachedHosts());
			NUnit.Framework.Assert.assertArrayEquals(names, loc.getNames());
			NUnit.Framework.Assert.assertArrayEquals(topologyPaths, loc.getTopologyPaths());
			NUnit.Framework.Assert.AreEqual(offset, loc.getOffset());
			NUnit.Framework.Assert.AreEqual(length, loc.getLength());
			NUnit.Framework.Assert.AreEqual(corrupt, loc.isCorrupt());
		}

		/// <summary>Call all the constructors and verify the delegation is working properly</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testBlockLocationConstructors()
		{
			//
			org.apache.hadoop.fs.BlockLocation loc;
			loc = new org.apache.hadoop.fs.BlockLocation();
			checkBlockLocation(loc);
			loc = new org.apache.hadoop.fs.BlockLocation(null, null, 1, 2);
			checkBlockLocation(loc, 1, 2, false);
			loc = new org.apache.hadoop.fs.BlockLocation(null, null, null, 1, 2);
			checkBlockLocation(loc, 1, 2, false);
			loc = new org.apache.hadoop.fs.BlockLocation(null, null, null, 1, 2, true);
			checkBlockLocation(loc, 1, 2, true);
			loc = new org.apache.hadoop.fs.BlockLocation(null, null, null, null, 1, 2, true);
			checkBlockLocation(loc, 1, 2, true);
		}

		/// <summary>Call each of the setters and verify</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testBlockLocationSetters()
		{
			org.apache.hadoop.fs.BlockLocation loc;
			loc = new org.apache.hadoop.fs.BlockLocation();
			// Test that null sets the empty array
			loc.setHosts(null);
			loc.setCachedHosts(null);
			loc.setNames(null);
			loc.setTopologyPaths(null);
			checkBlockLocation(loc);
			// Test that not-null gets set properly
			string[] names = new string[] { "name" };
			string[] hosts = new string[] { "host" };
			string[] cachedHosts = new string[] { "cachedHost" };
			string[] topologyPaths = new string[] { "path" };
			loc.setNames(names);
			loc.setHosts(hosts);
			loc.setCachedHosts(cachedHosts);
			loc.setTopologyPaths(topologyPaths);
			loc.setOffset(1);
			loc.setLength(2);
			loc.setCorrupt(true);
			checkBlockLocation(loc, names, hosts, cachedHosts, topologyPaths, 1, 2, true);
		}
	}
}
