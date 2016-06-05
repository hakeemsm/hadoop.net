using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests operations that modify ACLs.</summary>
	/// <remarks>
	/// Tests operations that modify ACLs.  All tests in this suite have been
	/// cross-validated against Linux setfacl/getfacl to check for consistency of the
	/// HDFS implementation.
	/// </remarks>
	public class TestAclTransformation
	{
		private static readonly IList<AclEntry> AclSpecTooLarge;

		static TestAclTransformation()
		{
			AclSpecTooLarge = Lists.NewArrayListWithCapacity(33);
			for (int i = 0; i < 33; ++i)
			{
				AclSpecTooLarge.AddItem(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.User, "user" + i, FsAction.All));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpec()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.
				Access, AclEntryType.User, "diana", FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.ReadExecute))).Add(
				AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Group, "execs", FsAction
				.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana"), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, "sales"));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(
				AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "execs", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecUnchanged()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "clark"), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, "execs"));
			NUnit.Framework.Assert.AreEqual(existing, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecAccessMaskCalculated()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana"));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecDefaultMaskCalculated()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "diana"));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecDefaultMaskPreserved()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana"));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "diana", FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecAccessMaskPreserved()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "diana"));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecAutomaticDefaultUser()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecAutomaticDefaultGroup()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(
				AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecAutomaticDefaultOther()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(
				AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(
				AclEntryScope.Default, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterAclEntriesByAclSpecEmptyAclSpec()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.
				Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList();
			NUnit.Framework.Assert.AreEqual(existing, AclTransformation.FilterAclEntriesByAclSpec
				(existing, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestFilterAclEntriesByAclSpecRemoveAccessMaskRequired()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask));
			AclTransformation.FilterAclEntriesByAclSpec(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestFilterAclEntriesByAclSpecRemoveDefaultMaskRequired()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask));
			AclTransformation.FilterAclEntriesByAclSpec(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestFilterAclEntriesByAclSpecInputTooLarge()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			AclTransformation.FilterAclEntriesByAclSpec(existing, AclSpecTooLarge);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterDefaultAclEntries()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.ReadExecute))).Add(
				AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask, FsAction.All)))
				.Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Other, FsAction.
				None))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.User, "bruce"
				, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.Group, "sales", FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.ReadExecute))).Build());
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(
				AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.ReadExecute))).Add(
				AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask, FsAction.All)))
				.Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Other, FsAction.
				None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.FilterDefaultAclEntries
				(existing));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestFilterDefaultAclEntriesUnchanged()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(existing, AclTransformation.FilterDefaultAclEntries
				(existing));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntries()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesUnchanged()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			NUnit.Framework.Assert.AreEqual(existing, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesMultipleNewBeforeExisting()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "diana"
				, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Mask, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "clark", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.ReadExecute));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "clark", FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesAccessMaskCalculated()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesDefaultMaskCalculated()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadExecute));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadExecute))).Add(
				AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.Group, FsAction.Read
				))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.Mask, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.Other, FsAction
				.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesDefaultMaskPreserved()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "diana", FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadExecute));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "diana"
				, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "diana", FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesAccessMaskPreserved()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "diana", FsAction.ReadExecute));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadExecute))).Add(
				AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.Group, FsAction.Read
				))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.Mask, FsAction
				.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.
				Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesAutomaticDefaultUser()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesAutomaticDefaultGroup()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesAutomaticDefaultOther()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesProvidedAccessMask()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesProvidedDefaultMask()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestMergeAclEntriesEmptyAclSpec()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.
				Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList();
			NUnit.Framework.Assert.AreEqual(existing, AclTransformation.MergeAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestMergeAclEntriesInputTooLarge()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			AclTransformation.MergeAclEntries(existing, AclSpecTooLarge);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestMergeAclEntriesResultTooLarge()
		{
			ImmutableList.Builder<AclEntry> aclBuilder = ((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All)));
			for (int i = 1; i <= 28; ++i)
			{
				aclBuilder.Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "user"
					 + i, FsAction.Read));
			}
			((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)aclBuilder.Add
				(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Group, FsAction.Read
				))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask, FsAction
				.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Other, FsAction
				.None));
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)aclBuilder.Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.Read));
			AclTransformation.MergeAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestMergeAclEntriesDuplicateEntries()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "clark", FsAction.Read), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.ReadExecute));
			AclTransformation.MergeAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestMergeAclEntriesNamedMask()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask, "bruce", FsAction.ReadExecute));
			AclTransformation.MergeAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestMergeAclEntriesNamedOther()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, "bruce", FsAction.ReadExecute));
			AclTransformation.MergeAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntries()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>
				().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, FsAction
				.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, "bruce"
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesUnchanged()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, "sales", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			NUnit.Framework.Assert.AreEqual(existing, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesAccessMaskCalculated()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesDefaultMaskCalculated()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "diana", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesDefaultMaskPreserved()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesAccessMaskPreserved()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)new ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new 
				ImmutableList.Builder<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, FsAction.All))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesAutomaticDefaultUser()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesAutomaticDefaultGroup()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.Read))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesAutomaticDefaultOther()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadWrite));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry(
				AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadWrite))).Add(AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceAclEntriesOnlyDefaults()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.Read));
			IList<AclEntry> expected = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder
				<AclEntry>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)(
				(ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "bruce", FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None))).Build());
			NUnit.Framework.Assert.AreEqual(expected, AclTransformation.ReplaceAclEntries(existing
				, aclSpec));
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesInputTooLarge()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			AclTransformation.ReplaceAclEntries(existing, AclSpecTooLarge);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesResultTooLarge()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayListWithCapacity(32);
			aclSpec.AddItem(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
				FsAction.All));
			for (int i = 1; i <= 29; ++i)
			{
				aclSpec.AddItem(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
					"user" + i, FsAction.Read));
			}
			aclSpec.AddItem(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Group, 
				FsAction.Read));
			aclSpec.AddItem(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Other, 
				FsAction.None));
			// The ACL spec now has 32 entries.  Automatic mask calculation will push it
			// over the limit to 33.
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesDuplicateEntries()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "clark", FsAction.Read), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesNamedMask()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask, "bruce", FsAction.ReadExecute));
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesNamedOther()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, "bruce", FsAction.ReadExecute));
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesMissingUser()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesMissingGroup()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		public virtual void TestReplaceAclEntriesMissingOther()
		{
			IList<AclEntry> existing = ((ImmutableList<AclEntry>)((ImmutableList.Builder<AclEntry
				>)((ImmutableList.Builder<AclEntry>)((ImmutableList.Builder<AclEntry>)new ImmutableList.Builder
				<AclEntry>().Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read))).Add(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None))).Build());
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "sales", FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Mask, FsAction.All));
			AclTransformation.ReplaceAclEntries(existing, aclSpec);
		}
	}
}
