using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestBlockLocation
	{
		private static readonly string[] EmptyStrArray = new string[0];

		/// <exception cref="System.Exception"/>
		private static void CheckBlockLocation(BlockLocation loc)
		{
			CheckBlockLocation(loc, 0, 0, false);
		}

		/// <exception cref="System.Exception"/>
		private static void CheckBlockLocation(BlockLocation loc, long offset, long length
			, bool corrupt)
		{
			CheckBlockLocation(loc, EmptyStrArray, EmptyStrArray, EmptyStrArray, EmptyStrArray
				, offset, length, corrupt);
		}

		/// <exception cref="System.Exception"/>
		private static void CheckBlockLocation(BlockLocation loc, string[] names, string[]
			 hosts, string[] cachedHosts, string[] topologyPaths, long offset, long length, 
			bool corrupt)
		{
			NUnit.Framework.Assert.IsNotNull(loc.GetHosts());
			NUnit.Framework.Assert.IsNotNull(loc.GetCachedHosts());
			NUnit.Framework.Assert.IsNotNull(loc.GetNames());
			NUnit.Framework.Assert.IsNotNull(loc.GetTopologyPaths());
			Assert.AssertArrayEquals(hosts, loc.GetHosts());
			Assert.AssertArrayEquals(cachedHosts, loc.GetCachedHosts());
			Assert.AssertArrayEquals(names, loc.GetNames());
			Assert.AssertArrayEquals(topologyPaths, loc.GetTopologyPaths());
			Assert.Equal(offset, loc.GetOffset());
			Assert.Equal(length, loc.GetLength());
			Assert.Equal(corrupt, loc.IsCorrupt());
		}

		/// <summary>Call all the constructors and verify the delegation is working properly</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBlockLocationConstructors()
		{
			//
			BlockLocation loc;
			loc = new BlockLocation();
			CheckBlockLocation(loc);
			loc = new BlockLocation(null, null, 1, 2);
			CheckBlockLocation(loc, 1, 2, false);
			loc = new BlockLocation(null, null, null, 1, 2);
			CheckBlockLocation(loc, 1, 2, false);
			loc = new BlockLocation(null, null, null, 1, 2, true);
			CheckBlockLocation(loc, 1, 2, true);
			loc = new BlockLocation(null, null, null, null, 1, 2, true);
			CheckBlockLocation(loc, 1, 2, true);
		}

		/// <summary>Call each of the setters and verify</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBlockLocationSetters()
		{
			BlockLocation loc;
			loc = new BlockLocation();
			// Test that null sets the empty array
			loc.SetHosts(null);
			loc.SetCachedHosts(null);
			loc.SetNames(null);
			loc.SetTopologyPaths(null);
			CheckBlockLocation(loc);
			// Test that not-null gets set properly
			string[] names = new string[] { "name" };
			string[] hosts = new string[] { "host" };
			string[] cachedHosts = new string[] { "cachedHost" };
			string[] topologyPaths = new string[] { "path" };
			loc.SetNames(names);
			loc.SetHosts(hosts);
			loc.SetCachedHosts(cachedHosts);
			loc.SetTopologyPaths(topologyPaths);
			loc.SetOffset(1);
			loc.SetLength(2);
			loc.SetCorrupt(true);
			CheckBlockLocation(loc, names, hosts, cachedHosts, topologyPaths, 1, 2, true);
		}
	}
}
