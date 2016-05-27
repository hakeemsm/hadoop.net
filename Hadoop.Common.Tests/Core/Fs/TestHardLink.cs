using System.IO;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This testing is fairly lightweight.</summary>
	/// <remarks>
	/// This testing is fairly lightweight.  Assumes HardLink routines will
	/// only be called when permissions etc are okay; no negative testing is
	/// provided.
	/// These tests all use
	/// "src" as the source directory,
	/// "tgt_one" as the target directory for single-file hardlinking, and
	/// "tgt_mult" as the target directory for multi-file hardlinking.
	/// Contents of them are/will be:
	/// dir:src:
	/// files: x1, x2, x3
	/// dir:tgt_one:
	/// files: x1 (linked to src/x1), y (linked to src/x2),
	/// x3 (linked to src/x3), x11 (also linked to src/x1)
	/// dir:tgt_mult:
	/// files: x1, x2, x3 (all linked to same name in src/)
	/// NOTICE: This test class only tests the functionality of the OS
	/// upon which the test is run! (although you're pretty safe with the
	/// unix-like OS's, unless a typo sneaks in.)
	/// </remarks>
	public class TestHardLink
	{
		public static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data") + "/test";

		private static readonly FilePath TestDir = new FilePath(TestRootDir, "hl");

		private static string Dir = "dir_";

		private static FilePath src = new FilePath(TestDir, Dir + "src");

		private static FilePath tgt_mult = new FilePath(TestDir, Dir + "tgt_mult");

		private static FilePath tgt_one = new FilePath(TestDir, Dir + "tgt_one");

		private static FilePath x1 = new FilePath(src, "x1");

		private static FilePath x2 = new FilePath(src, "x2");

		private static FilePath x3 = new FilePath(src, "x3");

		private static FilePath x1_one = new FilePath(tgt_one, "x1");

		private static FilePath y_one = new FilePath(tgt_one, "y");

		private static FilePath x3_one = new FilePath(tgt_one, "x3");

		private static FilePath x11_one = new FilePath(tgt_one, "x11");

		private static FilePath x1_mult = new FilePath(tgt_mult, "x1");

		private static FilePath x2_mult = new FilePath(tgt_mult, "x2");

		private static FilePath x3_mult = new FilePath(tgt_mult, "x3");

		private static string str1 = "11111";

		private static string str2 = "22222";

		private static string str3 = "33333";

		//define source and target directories
		//define source files
		//define File objects for the target hardlinks
		//content strings for file content testing
		/// <summary>Assure clean environment for start of testing</summary>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupClean()
		{
			//delete source and target directories if they exist
			FileUtil.FullyDelete(src);
			FileUtil.FullyDelete(tgt_one);
			FileUtil.FullyDelete(tgt_mult);
			//check that they are gone
			NUnit.Framework.Assert.IsFalse(src.Exists());
			NUnit.Framework.Assert.IsFalse(tgt_one.Exists());
			NUnit.Framework.Assert.IsFalse(tgt_mult.Exists());
		}

		/// <summary>Initialize clean environment for start of each test</summary>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupDirs()
		{
			//check that we start out with empty top-level test data directory
			NUnit.Framework.Assert.IsFalse(src.Exists());
			NUnit.Framework.Assert.IsFalse(tgt_one.Exists());
			NUnit.Framework.Assert.IsFalse(tgt_mult.Exists());
			//make the source and target directories
			src.Mkdirs();
			tgt_one.Mkdirs();
			tgt_mult.Mkdirs();
			//create the source files in src, with unique contents per file
			MakeNonEmptyFile(x1, str1);
			MakeNonEmptyFile(x2, str2);
			MakeNonEmptyFile(x3, str3);
			//validate
			ValidateSetup();
		}

		/// <summary>
		/// validate that
		/// <see>setupDirs()</see>
		/// produced the expected result
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void ValidateSetup()
		{
			//check existence of source directory and files
			NUnit.Framework.Assert.IsTrue(src.Exists());
			NUnit.Framework.Assert.AreEqual(3, src.List().Length);
			NUnit.Framework.Assert.IsTrue(x1.Exists());
			NUnit.Framework.Assert.IsTrue(x2.Exists());
			NUnit.Framework.Assert.IsTrue(x3.Exists());
			//check contents of source files
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1).Equals(str1));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x2).Equals(str2));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x3).Equals(str3));
			//check target directories exist and are empty
			NUnit.Framework.Assert.IsTrue(tgt_one.Exists());
			NUnit.Framework.Assert.IsTrue(tgt_mult.Exists());
			NUnit.Framework.Assert.AreEqual(0, tgt_one.List().Length);
			NUnit.Framework.Assert.AreEqual(0, tgt_mult.List().Length);
		}

		/// <summary>validate that single-file link operations produced the expected results</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ValidateTgtOne()
		{
			//check that target directory tgt_one ended up with expected four files
			NUnit.Framework.Assert.IsTrue(tgt_one.Exists());
			NUnit.Framework.Assert.AreEqual(4, tgt_one.List().Length);
			NUnit.Framework.Assert.IsTrue(x1_one.Exists());
			NUnit.Framework.Assert.IsTrue(x11_one.Exists());
			NUnit.Framework.Assert.IsTrue(y_one.Exists());
			NUnit.Framework.Assert.IsTrue(x3_one.Exists());
			//confirm the contents of those four files reflects the known contents
			//of the files they were hardlinked from.
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1_one).Equals(str1));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x11_one).Equals(str1));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(y_one).Equals(str2));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x3_one).Equals(str3));
		}

		/// <summary>validate that multi-file link operations produced the expected results</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ValidateTgtMult()
		{
			//check that target directory tgt_mult ended up with expected three files
			NUnit.Framework.Assert.IsTrue(tgt_mult.Exists());
			NUnit.Framework.Assert.AreEqual(3, tgt_mult.List().Length);
			NUnit.Framework.Assert.IsTrue(x1_mult.Exists());
			NUnit.Framework.Assert.IsTrue(x2_mult.Exists());
			NUnit.Framework.Assert.IsTrue(x3_mult.Exists());
			//confirm the contents of those three files reflects the known contents
			//of the files they were hardlinked from.
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1_mult).Equals(str1));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x2_mult).Equals(str2));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x3_mult).Equals(str3));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			SetupClean();
		}

		/// <exception cref="System.IO.IOException"/>
		private void MakeNonEmptyFile(FilePath file, string contents)
		{
			FileWriter fw = new FileWriter(file);
			fw.Write(contents);
			fw.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void AppendToFile(FilePath file, string contents)
		{
			FileWriter fw = new FileWriter(file, true);
			fw.Write(contents);
			fw.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private string FetchFileContents(FilePath file)
		{
			char[] buf = new char[20];
			FileReader fr = new FileReader(file);
			int cnt = fr.Read(buf);
			fr.Close();
			char[] result = Arrays.CopyOf(buf, cnt);
			return new string(result);
		}

		/// <summary>
		/// Sanity check the simplest case of HardLink.getLinkCount()
		/// to make sure we get back "1" for ordinary single-linked files.
		/// </summary>
		/// <remarks>
		/// Sanity check the simplest case of HardLink.getLinkCount()
		/// to make sure we get back "1" for ordinary single-linked files.
		/// Tests with multiply-linked files are in later test cases.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetLinkCount()
		{
			//at beginning of world, check that source files have link count "1"
			//since they haven't been hardlinked yet
			NUnit.Framework.Assert.AreEqual(1, HardLink.GetLinkCount(x1));
			NUnit.Framework.Assert.AreEqual(1, HardLink.GetLinkCount(x2));
			NUnit.Framework.Assert.AreEqual(1, HardLink.GetLinkCount(x3));
		}

		/// <summary>Test the single-file method HardLink.createHardLink().</summary>
		/// <remarks>
		/// Test the single-file method HardLink.createHardLink().
		/// Also tests getLinkCount() with values greater than one.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateHardLink()
		{
			//hardlink a single file and confirm expected result
			HardLink.CreateHardLink(x1, x1_one);
			NUnit.Framework.Assert.IsTrue(x1_one.Exists());
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x1));
			//x1 and x1_one are linked now 
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x1_one));
			//so they both have count "2"
			//confirm that x2, which we didn't change, still shows count "1"
			NUnit.Framework.Assert.AreEqual(1, HardLink.GetLinkCount(x2));
			//now do a few more
			HardLink.CreateHardLink(x2, y_one);
			HardLink.CreateHardLink(x3, x3_one);
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x2));
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x3));
			//create another link to a file that already has count 2
			HardLink.CreateHardLink(x1, x11_one);
			NUnit.Framework.Assert.AreEqual(3, HardLink.GetLinkCount(x1));
			//x1, x1_one, and x11_one
			NUnit.Framework.Assert.AreEqual(3, HardLink.GetLinkCount(x1_one));
			//are all linked, so they
			NUnit.Framework.Assert.AreEqual(3, HardLink.GetLinkCount(x11_one));
			//should all have count "3"
			//validate by contents
			ValidateTgtOne();
			//validate that change of content is reflected in the other linked files
			AppendToFile(x1_one, str3);
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1_one).Equals(str1 + str3));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x11_one).Equals(str1 + str3));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1).Equals(str1 + str3));
		}

		/*
		* Test the multi-file method HardLink.createHardLinkMult(),
		* multiple files within a directory into one target directory
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateHardLinkMult()
		{
			//hardlink a whole list of three files at once
			string[] fileNames = src.List();
			HardLink.CreateHardLinkMult(src, fileNames, tgt_mult);
			//validate by link count - each file has been linked once,
			//so each count is "2"
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x1));
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x2));
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x3));
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x1_mult));
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x2_mult));
			NUnit.Framework.Assert.AreEqual(2, HardLink.GetLinkCount(x3_mult));
			//validate by contents
			ValidateTgtMult();
			//validate that change of content is reflected in the other linked files
			AppendToFile(x1_mult, str3);
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1_mult).Equals(str1 + str3));
			NUnit.Framework.Assert.IsTrue(FetchFileContents(x1).Equals(str1 + str3));
		}

		/// <summary>Test createHardLinkMult() with empty list of files.</summary>
		/// <remarks>
		/// Test createHardLinkMult() with empty list of files.
		/// We use an extended version of the method call, that
		/// returns the number of System exec calls made, which should
		/// be zero in this case.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateHardLinkMultEmptyList()
		{
			string[] emptyList = new string[] {  };
			//test the case of empty file list
			HardLink.CreateHardLinkMult(src, emptyList, tgt_mult);
			//check nothing changed in the directory tree
			ValidateSetup();
		}

		/*
		* Assume that this test won't usually be run on a Windows box.
		* This test case allows testing of the correct syntax of the Windows
		* commands, even though they don't actually get executed on a non-Win box.
		* The basic idea is to have enough here that substantive changes will
		* fail and the author will fix and add to this test as appropriate.
		*
		* Depends on the HardLinkCGWin class and member fields being accessible
		* from this test method.
		*/
		[NUnit.Framework.Test]
		public virtual void TestWindowsSyntax()
		{
			//basic checks on array lengths
			NUnit.Framework.Assert.AreEqual(4, _T703671896.getLinkCountCommand.Length);
			//make sure "%f" was not munged
			NUnit.Framework.Assert.AreEqual(2, ("%f").Length);
			//make sure "\\%f" was munged correctly
			NUnit.Framework.Assert.AreEqual(3, ("\\%f").Length);
			NUnit.Framework.Assert.IsTrue(_T703671896.getLinkCountCommand[1].Equals("hardlink"
				));
			//make sure "-c%h" was not munged
			NUnit.Framework.Assert.AreEqual(4, ("-c%h").Length);
		}

		internal class _T703671896 : HardLink.HardLinkCGWin
		{
			internal _T703671896(TestHardLink _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestHardLink _enclosing;
		}
	}
}
