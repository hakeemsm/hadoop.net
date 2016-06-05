using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Test for
	/// <see cref="NameCache{K}"/>
	/// class
	/// </summary>
	public class TestNameCache
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDictionary()
		{
			// Create dictionary with useThreshold 2
			NameCache<string> cache = new NameCache<string>(2);
			string[] matching = new string[] { "part1", "part10000000", "fileabc", "abc", "filepart"
				 };
			string[] notMatching = new string[] { "spart1", "apart", "abcd", "def" };
			foreach (string s in matching)
			{
				// Add useThreshold times so the names are promoted to dictionary
				cache.Put(s);
				NUnit.Framework.Assert.IsTrue(s == cache.Put(s));
			}
			foreach (string s_1 in notMatching)
			{
				// Add < useThreshold times so the names are not promoted to dictionary
				cache.Put(s_1);
			}
			// Mark dictionary as initialized
			cache.Initialized();
			foreach (string s_2 in matching)
			{
				VerifyNameReuse(cache, s_2, true);
			}
			// Check dictionary size
			NUnit.Framework.Assert.AreEqual(matching.Length, cache.Size());
			foreach (string s_3 in notMatching)
			{
				VerifyNameReuse(cache, s_3, false);
			}
			cache.Reset();
			cache.Initialized();
			foreach (string s_4 in matching)
			{
				VerifyNameReuse(cache, s_4, false);
			}
			foreach (string s_5 in notMatching)
			{
				VerifyNameReuse(cache, s_5, false);
			}
		}

		private void VerifyNameReuse(NameCache<string> cache, string s, bool reused)
		{
			cache.Put(s);
			int lookupCount = cache.GetLookupCount();
			if (reused)
			{
				// Dictionary returns non null internal value
				NUnit.Framework.Assert.IsNotNull(cache.Put(s));
				// Successful lookup increments lookup count
				NUnit.Framework.Assert.AreEqual(lookupCount + 1, cache.GetLookupCount());
			}
			else
			{
				// Dictionary returns null - since name is not in the dictionary
				NUnit.Framework.Assert.IsNull(cache.Put(s));
				// Lookup count remains the same
				NUnit.Framework.Assert.AreEqual(lookupCount, cache.GetLookupCount());
			}
		}
	}
}
