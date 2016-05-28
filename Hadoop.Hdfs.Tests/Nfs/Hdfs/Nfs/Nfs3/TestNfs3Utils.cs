using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestNfs3Utils
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAccessRightsForUserGroup()
		{
			Nfs3FileAttributes attr = Org.Mockito.Mockito.Mock<Nfs3FileAttributes>();
			Org.Mockito.Mockito.When(attr.GetUid()).ThenReturn(2);
			Org.Mockito.Mockito.When(attr.GetGid()).ThenReturn(3);
			Org.Mockito.Mockito.When(attr.GetMode()).ThenReturn(448);
			// 700
			Org.Mockito.Mockito.When(attr.GetType()).ThenReturn(NfsFileType.Nfsreg.ToValue());
			NUnit.Framework.Assert.AreEqual("No access should be allowed as UID does not match attribute over mode 700"
				, 0, Nfs3Utils.GetAccessRightsForUserGroup(3, 3, null, attr));
			Org.Mockito.Mockito.When(attr.GetUid()).ThenReturn(2);
			Org.Mockito.Mockito.When(attr.GetGid()).ThenReturn(3);
			Org.Mockito.Mockito.When(attr.GetMode()).ThenReturn(56);
			// 070
			Org.Mockito.Mockito.When(attr.GetType()).ThenReturn(NfsFileType.Nfsreg.ToValue());
			NUnit.Framework.Assert.AreEqual("No access should be allowed as GID does not match attribute over mode 070"
				, 0, Nfs3Utils.GetAccessRightsForUserGroup(2, 4, null, attr));
			Org.Mockito.Mockito.When(attr.GetUid()).ThenReturn(2);
			Org.Mockito.Mockito.When(attr.GetGid()).ThenReturn(3);
			Org.Mockito.Mockito.When(attr.GetMode()).ThenReturn(7);
			// 007
			Org.Mockito.Mockito.When(attr.GetType()).ThenReturn(NfsFileType.Nfsreg.ToValue());
			NUnit.Framework.Assert.AreEqual("Access should be allowed as mode is 007 and UID/GID do not match"
				, 61, Nfs3Utils.GetAccessRightsForUserGroup(1, 4, new int[] { 5, 6 }, attr));
			/* RWX */
			Org.Mockito.Mockito.When(attr.GetUid()).ThenReturn(2);
			Org.Mockito.Mockito.When(attr.GetGid()).ThenReturn(10);
			Org.Mockito.Mockito.When(attr.GetMode()).ThenReturn(288);
			// 440
			Org.Mockito.Mockito.When(attr.GetType()).ThenReturn(NfsFileType.Nfsreg.ToValue());
			NUnit.Framework.Assert.AreEqual("Access should be allowed as mode is 440 and Aux GID does match"
				, 1, Nfs3Utils.GetAccessRightsForUserGroup(3, 4, new int[] { 5, 16, 10 }, attr));
			/* R */
			Org.Mockito.Mockito.When(attr.GetUid()).ThenReturn(2);
			Org.Mockito.Mockito.When(attr.GetGid()).ThenReturn(10);
			Org.Mockito.Mockito.When(attr.GetMode()).ThenReturn(448);
			// 700
			Org.Mockito.Mockito.When(attr.GetType()).ThenReturn(NfsFileType.Nfsdir.ToValue());
			NUnit.Framework.Assert.AreEqual("Access should be allowed for dir as mode is 700 and UID does match"
				, 31, Nfs3Utils.GetAccessRightsForUserGroup(2, 4, new int[] { 5, 16, 10 }, attr)
				);
			/* Lookup */
			NUnit.Framework.Assert.AreEqual("No access should be allowed for dir as mode is 700 even though GID does match"
				, 0, Nfs3Utils.GetAccessRightsForUserGroup(3, 10, new int[] { 5, 16, 4 }, attr));
			NUnit.Framework.Assert.AreEqual("No access should be allowed for dir as mode is 700 even though AuxGID does match"
				, 0, Nfs3Utils.GetAccessRightsForUserGroup(3, 20, new int[] { 5, 10 }, attr));
			Org.Mockito.Mockito.When(attr.GetUid()).ThenReturn(2);
			Org.Mockito.Mockito.When(attr.GetGid()).ThenReturn(10);
			Org.Mockito.Mockito.When(attr.GetMode()).ThenReturn(457);
			// 711
			Org.Mockito.Mockito.When(attr.GetType()).ThenReturn(NfsFileType.Nfsdir.ToValue());
			NUnit.Framework.Assert.AreEqual("Access should be allowed for dir as mode is 711 and GID matches"
				, 2, Nfs3Utils.GetAccessRightsForUserGroup(3, 10, new int[] { 5, 16, 11 }, attr)
				);
		}
		/* Lookup */
	}
}
