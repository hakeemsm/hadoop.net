using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestGetBlockLocations
	{
		private const string FileName = "foo";

		private const string FilePath = "/" + FileName;

		private const long MockInodeId = 16386;

		private const string ReservedPath = "/.reserved/.inodes/" + MockInodeId;

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolveReservedPath()
		{
			FSNamesystem fsn = SetupFileSystem();
			FSEditLog editlog = fsn.GetEditLog();
			fsn.GetBlockLocations("dummy", ReservedPath, 0, 1024);
			Org.Mockito.Mockito.Verify(editlog).LogTimes(Matchers.Eq(FilePath), Matchers.AnyLong
				(), Matchers.AnyLong());
			fsn.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetBlockLocationsRacingWithDelete()
		{
			FSNamesystem fsn = Org.Mockito.Mockito.Spy(SetupFileSystem());
			FSDirectory fsd = fsn.GetFSDirectory();
			FSEditLog editlog = fsn.GetEditLog();
			Org.Mockito.Mockito.DoAnswer(new _Answer_67(fsd)).When(fsn).WriteLock();
			fsn.GetBlockLocations("dummy", ReservedPath, 0, 1024);
			Org.Mockito.Mockito.Verify(editlog, Org.Mockito.Mockito.Never()).LogTimes(Matchers.AnyString
				(), Matchers.AnyLong(), Matchers.AnyLong());
			fsn.Close();
		}

		private sealed class _Answer_67 : Answer<Void>
		{
			public _Answer_67(FSDirectory fsd)
			{
				this.fsd = fsd;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				INodesInPath iip = fsd.GetINodesInPath(TestGetBlockLocations.FilePath, true);
				FSDirDeleteOp.Delete(fsd, iip, new INode.BlocksMapUpdateInfo(), new AList<INode>(
					), Time.Now());
				invocation.CallRealMethod();
				return null;
			}

			private readonly FSDirectory fsd;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetBlockLocationsRacingWithRename()
		{
			FSNamesystem fsn = Org.Mockito.Mockito.Spy(SetupFileSystem());
			FSDirectory fsd = fsn.GetFSDirectory();
			FSEditLog editlog = fsn.GetEditLog();
			string DstPath = "/bar";
			bool[] renamed = new bool[1];
			Org.Mockito.Mockito.DoAnswer(new _Answer_92(renamed, fsd, DstPath)).When(fsn).WriteLock
				();
			fsn.GetBlockLocations("dummy", ReservedPath, 0, 1024);
			Org.Mockito.Mockito.Verify(editlog).LogTimes(Matchers.Eq(DstPath), Matchers.AnyLong
				(), Matchers.AnyLong());
			fsn.Close();
		}

		private sealed class _Answer_92 : Answer<Void>
		{
			public _Answer_92(bool[] renamed, FSDirectory fsd, string DstPath)
			{
				this.renamed = renamed;
				this.fsd = fsd;
				this.DstPath = DstPath;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				invocation.CallRealMethod();
				if (!renamed[0])
				{
					FSDirRenameOp.RenameTo(fsd, fsd.GetPermissionChecker(), TestGetBlockLocations.FilePath
						, DstPath, new INode.BlocksMapUpdateInfo(), false);
					renamed[0] = true;
				}
				return null;
			}

			private readonly bool[] renamed;

			private readonly FSDirectory fsd;

			private readonly string DstPath;
		}

		/// <exception cref="System.IO.IOException"/>
		private static FSNamesystem SetupFileSystem()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, 1L);
			FSEditLog editlog = Org.Mockito.Mockito.Mock<FSEditLog>();
			FSImage image = Org.Mockito.Mockito.Mock<FSImage>();
			Org.Mockito.Mockito.When(image.GetEditLog()).ThenReturn(editlog);
			FSNamesystem fsn = new FSNamesystem(conf, image, true);
			FSDirectory fsd = fsn.GetFSDirectory();
			INodesInPath iip = fsd.GetINodesInPath("/", true);
			PermissionStatus perm = new PermissionStatus("hdfs", "supergroup", FsPermission.CreateImmutable
				((short)unchecked((int)(0x1ff))));
			INodeFile file = new INodeFile(MockInodeId, Sharpen.Runtime.GetBytesForString(FileName
				, Charsets.Utf8), perm, 1, 1, new BlockInfoContiguous[] {  }, (short)1, DFSConfigKeys
				.DfsBlockSizeDefault);
			fsn.GetFSDirectory().AddINode(iip, file);
			return fsn;
		}
	}
}
