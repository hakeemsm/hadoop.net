using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Filecache
{
	public class TestURIFragments
	{
		/// <summary>
		/// Tests
		/// <see>DistributedCache#checkURIs(URI[], URI[]).</see>
		/// </summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestURIs()
		{
			NUnit.Framework.Assert.IsTrue(DistributedCache.CheckURIs(null, null));
			// uris with no fragments
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile.txt"
				) }, null));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(null, new URI[] { new URI
				("file://foo/bar/myCacheArchive.txt") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file"
				), new URI("file://foo/bar/myCacheFile2.txt") }, null));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(null, new URI[] { new URI
				("file://foo/bar/myCacheArchive1.txt"), new URI("file://foo/bar/myCacheArchive2.txt#archive"
				) }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile.txt"
				) }, new URI[] { new URI("file://foo/bar/myCacheArchive.txt") }));
			// conflicts in fragment names
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file"
				), new URI("file://foo/bar/myCacheFile2.txt#file") }, null));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(null, new URI[] { new URI
				("file://foo/bar/myCacheArchive1.txt#archive"), new URI("file://foo/bar/myCacheArchive2.txt#archive"
				) }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile.txt#cache"
				) }, new URI[] { new URI("file://foo/bar/myCacheArchive.txt#cache") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file1"
				), new URI("file://foo/bar/myCacheFile2.txt#file2") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#archive"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file"
				), new URI("file://foo/bar/myCacheFile2.txt#file") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#archive1"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file1"
				), new URI("file://foo/bar/myCacheFile2.txt#cache") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#cache"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
			// test ignore case
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file"
				), new URI("file://foo/bar/myCacheFile2.txt#FILE") }, null));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(null, new URI[] { new URI
				("file://foo/bar/myCacheArchive1.txt#archive"), new URI("file://foo/bar/myCacheArchive2.txt#ARCHIVE"
				) }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile.txt#cache"
				) }, new URI[] { new URI("file://foo/bar/myCacheArchive.txt#CACHE") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file1"
				), new URI("file://foo/bar/myCacheFile2.txt#file2") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#ARCHIVE"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#FILE"
				), new URI("file://foo/bar/myCacheFile2.txt#file") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#archive1"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
			NUnit.Framework.Assert.IsFalse(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file1"
				), new URI("file://foo/bar/myCacheFile2.txt#CACHE") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#cache"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
			// allowed uri combinations
			NUnit.Framework.Assert.IsTrue(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file1"
				), new URI("file://foo/bar/myCacheFile2.txt#file2") }, null));
			NUnit.Framework.Assert.IsTrue(DistributedCache.CheckURIs(null, new URI[] { new URI
				("file://foo/bar/myCacheArchive1.txt#archive1"), new URI("file://foo/bar/myCacheArchive2.txt#archive2"
				) }));
			NUnit.Framework.Assert.IsTrue(DistributedCache.CheckURIs(new URI[] { new URI("file://foo/bar/myCacheFile1.txt#file1"
				), new URI("file://foo/bar/myCacheFile2.txt#file2") }, new URI[] { new URI("file://foo/bar/myCacheArchive1.txt#archive1"
				), new URI("file://foo/bar/myCacheArchive2.txt#archive2") }));
		}
	}
}
