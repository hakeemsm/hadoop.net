using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Class to pack an AclEntry into an integer.</summary>
	/// <remarks>
	/// Class to pack an AclEntry into an integer. <br />
	/// An ACL entry is represented by a 32-bit integer in Big Endian format. <br />
	/// The bits can be divided in four segments: <br />
	/// [0:1) || [1:3) || [3:6) || [6:7) || [7:32) <br />
	/// <br />
	/// [0:1) -- the scope of the entry (AclEntryScope) <br />
	/// [1:3) -- the type of the entry (AclEntryType) <br />
	/// [3:6) -- the permission of the entry (FsAction) <br />
	/// [6:7) -- A flag to indicate whether Named entry or not <br />
	/// [7:32) -- the name of the entry, which is an ID that points to a <br />
	/// string in the StringTableSection. <br />
	/// </remarks>
	[System.Serializable]
	public sealed class AclEntryStatusFormat
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			 Scope = new Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat(null, 1
			);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			 Type = new Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat(Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			.Scope.Bits, 2);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			 Permission = new Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat(Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			.Type.Bits, 3);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			 NamedEntryCheck = new Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			(Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Permission.Bits, 1);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			 Name = new Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat(Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
			.NamedEntryCheck.Bits, 25);

		private readonly LongBitFormat Bits;

		private AclEntryStatusFormat(LongBitFormat previous, int length)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Bits = new LongBitFormat
				(Name(), previous, length, 0);
		}

		internal static AclEntryScope GetScope(int aclEntry)
		{
			int ordinal = (int)Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Scope
				.Bits.Retrieve(aclEntry);
			return AclEntryScope.Values()[ordinal];
		}

		internal static AclEntryType GetType(int aclEntry)
		{
			int ordinal = (int)Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Type
				.Bits.Retrieve(aclEntry);
			return AclEntryType.Values()[ordinal];
		}

		internal static FsAction GetPermission(int aclEntry)
		{
			int ordinal = (int)Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Permission
				.Bits.Retrieve(aclEntry);
			return FsAction.Values()[ordinal];
		}

		internal static string GetName(int aclEntry)
		{
			int nameExists = (int)Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat
				.NamedEntryCheck.Bits.Retrieve(aclEntry);
			if (nameExists == 0)
			{
				return null;
			}
			int id = (int)Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Name.Bits
				.Retrieve(aclEntry);
			AclEntryType type = GetType(aclEntry);
			if (type == AclEntryType.User)
			{
				return SerialNumberManager.Instance.GetUser(id);
			}
			else
			{
				if (type == AclEntryType.Group)
				{
					return SerialNumberManager.Instance.GetGroup(id);
				}
			}
			return null;
		}

		internal static int ToInt(AclEntry aclEntry)
		{
			long aclEntryInt = 0;
			aclEntryInt = Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Scope.Bits
				.Combine((int)(aclEntry.GetScope()), aclEntryInt);
			aclEntryInt = Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Type.Bits
				.Combine((int)(aclEntry.GetType()), aclEntryInt);
			aclEntryInt = Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Permission
				.Bits.Combine((int)(aclEntry.GetPermission()), aclEntryInt);
			if (aclEntry.GetName() != null)
			{
				aclEntryInt = Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.NamedEntryCheck
					.Bits.Combine(1, aclEntryInt);
				if (aclEntry.GetType() == AclEntryType.User)
				{
					int userId = SerialNumberManager.Instance.GetUserSerialNumber(aclEntry.GetName());
					aclEntryInt = Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Name.Bits
						.Combine(userId, aclEntryInt);
				}
				else
				{
					if (aclEntry.GetType() == AclEntryType.Group)
					{
						int groupId = SerialNumberManager.Instance.GetGroupSerialNumber(aclEntry.GetName(
							));
						aclEntryInt = Org.Apache.Hadoop.Hdfs.Server.Namenode.AclEntryStatusFormat.Name.Bits
							.Combine(groupId, aclEntryInt);
					}
				}
			}
			return (int)aclEntryInt;
		}

		internal static AclEntry ToAclEntry(int aclEntry)
		{
			AclEntry.Builder builder = new AclEntry.Builder();
			builder.SetScope(GetScope(aclEntry)).SetType(GetType(aclEntry)).SetPermission(GetPermission
				(aclEntry));
			if (GetName(aclEntry) != null)
			{
				builder.SetName(GetName(aclEntry));
			}
			return builder.Build();
		}

		public static int[] ToInt(IList<AclEntry> aclEntries)
		{
			int[] entries = new int[aclEntries.Count];
			for (int i = 0; i < entries.Length; i++)
			{
				entries[i] = ToInt(aclEntries[i]);
			}
			return entries;
		}

		public static ImmutableList<AclEntry> ToAclEntries(int[] entries)
		{
			ImmutableList.Builder<AclEntry> b = new ImmutableList.Builder<AclEntry>();
			foreach (int entry in entries)
			{
				AclEntry aclEntry = ToAclEntry(entry);
				b.Add(aclEntry);
			}
			return ((ImmutableList<AclEntry>)b.Build());
		}
	}
}
