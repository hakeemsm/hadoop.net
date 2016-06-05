using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Jobhistory
{
	public class TestJobHistoryUtils
	{
		internal static readonly string TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			)).GetAbsolutePath();

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetHistoryDirsForCleaning()
		{
			Path pRoot = new Path(TestDir, "org.apache.hadoop.mapreduce.v2.jobhistory." + "TestJobHistoryUtils.testGetHistoryDirsForCleaning"
				);
			FileContext fc = FileContext.GetFileContext();
			Calendar cCal = Calendar.GetInstance();
			int year = 2013;
			int month = 7;
			int day = 21;
			cCal.Set(year, month - 1, day, 1, 0);
			long cutoff = cCal.GetTimeInMillis();
			ClearDir(fc, pRoot);
			Path pId00 = CreatePath(fc, pRoot, year, month, day, "000000");
			Path pId01 = CreatePath(fc, pRoot, year, month, day + 1, "000001");
			Path pId02 = CreatePath(fc, pRoot, year, month, day - 1, "000002");
			Path pId03 = CreatePath(fc, pRoot, year, month + 1, day, "000003");
			Path pId04 = CreatePath(fc, pRoot, year, month + 1, day + 1, "000004");
			Path pId05 = CreatePath(fc, pRoot, year, month + 1, day - 1, "000005");
			Path pId06 = CreatePath(fc, pRoot, year, month - 1, day, "000006");
			Path pId07 = CreatePath(fc, pRoot, year, month - 1, day + 1, "000007");
			Path pId08 = CreatePath(fc, pRoot, year, month - 1, day - 1, "000008");
			Path pId09 = CreatePath(fc, pRoot, year + 1, month, day, "000009");
			Path pId10 = CreatePath(fc, pRoot, year + 1, month, day + 1, "000010");
			Path pId11 = CreatePath(fc, pRoot, year + 1, month, day - 1, "000011");
			Path pId12 = CreatePath(fc, pRoot, year + 1, month + 1, day, "000012");
			Path pId13 = CreatePath(fc, pRoot, year + 1, month + 1, day + 1, "000013");
			Path pId14 = CreatePath(fc, pRoot, year + 1, month + 1, day - 1, "000014");
			Path pId15 = CreatePath(fc, pRoot, year + 1, month - 1, day, "000015");
			Path pId16 = CreatePath(fc, pRoot, year + 1, month - 1, day + 1, "000016");
			Path pId17 = CreatePath(fc, pRoot, year + 1, month - 1, day - 1, "000017");
			Path pId18 = CreatePath(fc, pRoot, year - 1, month, day, "000018");
			Path pId19 = CreatePath(fc, pRoot, year - 1, month, day + 1, "000019");
			Path pId20 = CreatePath(fc, pRoot, year - 1, month, day - 1, "000020");
			Path pId21 = CreatePath(fc, pRoot, year - 1, month + 1, day, "000021");
			Path pId22 = CreatePath(fc, pRoot, year - 1, month + 1, day + 1, "000022");
			Path pId23 = CreatePath(fc, pRoot, year - 1, month + 1, day - 1, "000023");
			Path pId24 = CreatePath(fc, pRoot, year - 1, month - 1, day, "000024");
			Path pId25 = CreatePath(fc, pRoot, year - 1, month - 1, day + 1, "000025");
			Path pId26 = CreatePath(fc, pRoot, year - 1, month - 1, day - 1, "000026");
			// non-expected names should be ignored without problems
			Path pId27 = CreatePath(fc, pRoot, "foo", string.Empty + month, string.Empty + day
				, "000027");
			Path pId28 = CreatePath(fc, pRoot, string.Empty + year, "foo", string.Empty + day
				, "000028");
			Path pId29 = CreatePath(fc, pRoot, string.Empty + year, string.Empty + month, "foo"
				, "000029");
			IList<FileStatus> dirs = JobHistoryUtils.GetHistoryDirsForCleaning(fc, pRoot, cutoff
				);
			dirs.Sort();
			NUnit.Framework.Assert.AreEqual(14, dirs.Count);
			NUnit.Framework.Assert.AreEqual(pId26.ToUri().GetPath(), dirs[0].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId24.ToUri().GetPath(), dirs[1].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId25.ToUri().GetPath(), dirs[2].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId20.ToUri().GetPath(), dirs[3].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId18.ToUri().GetPath(), dirs[4].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId19.ToUri().GetPath(), dirs[5].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId23.ToUri().GetPath(), dirs[6].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId21.ToUri().GetPath(), dirs[7].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId22.ToUri().GetPath(), dirs[8].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId08.ToUri().GetPath(), dirs[9].GetPath().ToUri(
				).GetPath());
			NUnit.Framework.Assert.AreEqual(pId06.ToUri().GetPath(), dirs[10].GetPath().ToUri
				().GetPath());
			NUnit.Framework.Assert.AreEqual(pId07.ToUri().GetPath(), dirs[11].GetPath().ToUri
				().GetPath());
			NUnit.Framework.Assert.AreEqual(pId02.ToUri().GetPath(), dirs[12].GetPath().ToUri
				().GetPath());
			NUnit.Framework.Assert.AreEqual(pId00.ToUri().GetPath(), dirs[13].GetPath().ToUri
				().GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		private void ClearDir(FileContext fc, Path p)
		{
			try
			{
				fc.Delete(p, true);
			}
			catch (FileNotFoundException)
			{
			}
			// ignore
			fc.Mkdir(p, FsPermission.GetDirDefault(), false);
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreatePath(FileContext fc, Path root, int year, int month, int day, 
			string id)
		{
			Path path = new Path(root, year + Path.Separator + month + Path.Separator + day +
				 Path.Separator + id);
			fc.Mkdir(path, FsPermission.GetDirDefault(), true);
			return path;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreatePath(FileContext fc, Path root, string year, string month, string
			 day, string id)
		{
			Path path = new Path(root, year + Path.Separator + month + Path.Separator + day +
				 Path.Separator + id);
			fc.Mkdir(path, FsPermission.GetDirDefault(), true);
			return path;
		}
	}
}
