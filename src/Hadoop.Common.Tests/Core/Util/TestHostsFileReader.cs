using System.IO;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Util
{
	public class TestHostsFileReader
	{
		internal readonly string HostsTestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).GetAbsolutePath();

		internal FilePath ExcludesFile;

		internal FilePath IncludesFile;

		internal string excludesFile = HostsTestDir + "/dfs.exclude";

		internal string includesFile = HostsTestDir + "/dfs.include";

		/*
		* Test for HostsFileReader.java
		*
		*/
		// Using /test/build/data/tmp directory to store temprory files
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			// Delete test files after running tests
			ExcludesFile.Delete();
			IncludesFile.Delete();
		}

		/*
		* 1.Create dfs.exclude,dfs.include file
		* 2.Write host names per line
		* 3.Write comments starting with #
		* 4.Close file
		* 5.Compare if number of hosts reported by HostsFileReader
		*   are equal to the number of hosts written
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHostsFileReader()
		{
			FileWriter efw = new FileWriter(excludesFile);
			FileWriter ifw = new FileWriter(includesFile);
			efw.Write("#DFS-Hosts-excluded\n");
			efw.Write("somehost1\n");
			efw.Write("#This-is-comment\n");
			efw.Write("somehost2\n");
			efw.Write("somehost3 # host3\n");
			efw.Write("somehost4\n");
			efw.Write("somehost4 somehost5\n");
			efw.Close();
			ifw.Write("#Hosts-in-DFS\n");
			ifw.Write("somehost1\n");
			ifw.Write("somehost2\n");
			ifw.Write("somehost3\n");
			ifw.Write("#This-is-comment\n");
			ifw.Write("somehost4 # host4\n");
			ifw.Write("somehost4 somehost5\n");
			ifw.Close();
			HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
			int includesLen = hfp.GetHosts().Count;
			int excludesLen = hfp.GetExcludedHosts().Count;
			Assert.Equal(5, includesLen);
			Assert.Equal(5, excludesLen);
			Assert.True(hfp.GetHosts().Contains("somehost5"));
			NUnit.Framework.Assert.IsFalse(hfp.GetHosts().Contains("host3"));
			Assert.True(hfp.GetExcludedHosts().Contains("somehost5"));
			NUnit.Framework.Assert.IsFalse(hfp.GetExcludedHosts().Contains("host4"));
		}

		/*
		* Test creating a new HostsFileReader with nonexistent files
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreateHostFileReaderWithNonexistentFile()
		{
			try
			{
				new HostsFileReader(HostsTestDir + "/doesnt-exist", HostsTestDir + "/doesnt-exist"
					);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Exception as expected
		/*
		* Test refreshing an existing HostsFileReader with an includes file that no longer exists
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRefreshHostFileReaderWithNonexistentFile()
		{
			FileWriter efw = new FileWriter(excludesFile);
			FileWriter ifw = new FileWriter(includesFile);
			efw.Close();
			ifw.Close();
			HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
			Assert.True(IncludesFile.Delete());
			try
			{
				hfp.Refresh();
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Exception as expected
		/*
		* Test for null file
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHostFileReaderWithNull()
		{
			FileWriter efw = new FileWriter(excludesFile);
			FileWriter ifw = new FileWriter(includesFile);
			efw.Close();
			ifw.Close();
			HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
			int includesLen = hfp.GetHosts().Count;
			int excludesLen = hfp.GetExcludedHosts().Count;
			// TestCase1: Check if lines beginning with # are ignored
			Assert.Equal(0, includesLen);
			Assert.Equal(0, excludesLen);
			// TestCase2: Check if given host names are reported by getHosts and
			// getExcludedHosts
			NUnit.Framework.Assert.IsFalse(hfp.GetHosts().Contains("somehost5"));
			NUnit.Framework.Assert.IsFalse(hfp.GetExcludedHosts().Contains("somehost5"));
		}

		/*
		* Check if only comments can be written to hosts file
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHostFileReaderWithCommentsOnly()
		{
			FileWriter efw = new FileWriter(excludesFile);
			FileWriter ifw = new FileWriter(includesFile);
			efw.Write("#DFS-Hosts-excluded\n");
			efw.Close();
			ifw.Write("#Hosts-in-DFS\n");
			ifw.Close();
			HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
			int includesLen = hfp.GetHosts().Count;
			int excludesLen = hfp.GetExcludedHosts().Count;
			Assert.Equal(0, includesLen);
			Assert.Equal(0, excludesLen);
			NUnit.Framework.Assert.IsFalse(hfp.GetHosts().Contains("somehost5"));
			NUnit.Framework.Assert.IsFalse(hfp.GetExcludedHosts().Contains("somehost5"));
		}

		/*
		* Test if spaces are allowed in host names
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHostFileReaderWithSpaces()
		{
			FileWriter efw = new FileWriter(excludesFile);
			FileWriter ifw = new FileWriter(includesFile);
			efw.Write("#DFS-Hosts-excluded\n");
			efw.Write("   somehost somehost2");
			efw.Write("   somehost3 # somehost4");
			efw.Close();
			ifw.Write("#Hosts-in-DFS\n");
			ifw.Write("   somehost somehost2");
			ifw.Write("   somehost3 # somehost4");
			ifw.Close();
			HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
			int includesLen = hfp.GetHosts().Count;
			int excludesLen = hfp.GetExcludedHosts().Count;
			Assert.Equal(3, includesLen);
			Assert.Equal(3, excludesLen);
			Assert.True(hfp.GetHosts().Contains("somehost3"));
			NUnit.Framework.Assert.IsFalse(hfp.GetHosts().Contains("somehost5"));
			NUnit.Framework.Assert.IsFalse(hfp.GetHosts().Contains("somehost4"));
			Assert.True(hfp.GetExcludedHosts().Contains("somehost3"));
			NUnit.Framework.Assert.IsFalse(hfp.GetExcludedHosts().Contains("somehost5"));
			NUnit.Framework.Assert.IsFalse(hfp.GetExcludedHosts().Contains("somehost4"));
		}

		/*
		* Test if spaces , tabs and new lines are allowed
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHostFileReaderWithTabs()
		{
			FileWriter efw = new FileWriter(excludesFile);
			FileWriter ifw = new FileWriter(includesFile);
			efw.Write("#DFS-Hosts-excluded\n");
			efw.Write("     \n");
			efw.Write("   somehost \t somehost2 \n somehost4");
			efw.Write("   somehost3 \t # somehost5");
			efw.Close();
			ifw.Write("#Hosts-in-DFS\n");
			ifw.Write("     \n");
			ifw.Write("   somehost \t  somehost2 \n somehost4");
			ifw.Write("   somehost3 \t # somehost5");
			ifw.Close();
			HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
			int includesLen = hfp.GetHosts().Count;
			int excludesLen = hfp.GetExcludedHosts().Count;
			Assert.Equal(4, includesLen);
			Assert.Equal(4, excludesLen);
			Assert.True(hfp.GetHosts().Contains("somehost2"));
			NUnit.Framework.Assert.IsFalse(hfp.GetHosts().Contains("somehost5"));
			Assert.True(hfp.GetExcludedHosts().Contains("somehost2"));
			NUnit.Framework.Assert.IsFalse(hfp.GetExcludedHosts().Contains("somehost5"));
		}

		public TestHostsFileReader()
		{
			ExcludesFile = new FilePath(HostsTestDir, "dfs.exclude");
			IncludesFile = new FilePath(HostsTestDir, "dfs.include");
		}
	}
}
