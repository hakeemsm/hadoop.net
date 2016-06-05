using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Op codes for edits file</summary>
	[System.Serializable]
	public sealed class FSEditLogOpCodes
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAdd
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)0
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRenameOld
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)1
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpDelete
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)2
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpMkdir
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)3
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetReplication
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)4
			));

		[Obsolete]
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpDatanodeAdd
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)5
			));

		[Obsolete]
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpDatanodeRemove
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)6
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetPermissions
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)7
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetOwner
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)8
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpClose
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)9
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetGenstampV1
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)10
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetNsQuota
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)11
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpClearNsQuota
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)12
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpTimes
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)13
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetQuota
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)14
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRename
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)15
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpConcatDelete
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)16
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSymlink
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)17
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpGetDelegationToken
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)18
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRenewDelegationToken
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)19
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpCancelDelegationToken
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)20
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpUpdateMasterKey
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)21
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpReassignLease
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)22
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpEndLogSegment
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)23
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpStartLogSegment
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)24
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpUpdateBlocks
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)25
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpCreateSnapshot
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)26
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpDeleteSnapshot
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)27
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRenameSnapshot
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)28
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAllowSnapshot
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)29
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpDisallowSnapshot
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)30
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetGenstampV2
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)31
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAllocateBlockId
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)32
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAddBlock
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)33
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAddCacheDirective
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)34
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRemoveCacheDirective
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)35
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAddCachePool
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)36
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpModifyCachePool
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)37
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRemoveCachePool
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)38
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpModifyCacheDirective
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)39
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetAcl
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)40
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRollingUpgradeStart
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)41
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRollingUpgradeFinalize
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)42
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetXattr
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)43
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpRemoveXattr
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)44
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetStoragePolicy
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)45
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpTruncate
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)46
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpAppend
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)47
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpSetQuotaByStoragetype
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)48
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes OpInvalid
			 = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes(unchecked((byte)-
			1));

		private readonly byte opCode;

		/// <summary>Constructor</summary>
		/// <param name="opCode">byte value of constructed enum</param>
		internal FSEditLogOpCodes(byte opCode)
		{
			// last op code in file
			// deprecated operation
			// obsolete
			// obsolete
			// obsolete
			// obsolete
			// set atime, mtime
			// filecontext rename
			// concat files
			// Note that the current range of the valid OP code is 0~127
			this.opCode = opCode;
		}

		/// <summary>return the byte value of the enum</summary>
		/// <returns>the byte value of the enum</returns>
		public byte GetOpCode()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes.opCode;
		}

		private static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes[]
			 Values;

		static FSEditLogOpCodes()
		{
			byte max = 0;
			foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes code in Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes
				.Values())
			{
				if (code.GetOpCode() > max)
				{
					max = code.GetOpCode();
				}
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes.Values = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes
				[max + 1];
			foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes code_1 in Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes
				.Values())
			{
				if (code_1.GetOpCode() >= 0)
				{
					Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes.Values[code_1.GetOpCode()
						] = code_1;
				}
			}
		}

		/// <summary>Converts byte to FSEditLogOpCodes enum value</summary>
		/// <param name="opCode">get enum for this opCode</param>
		/// <returns>enum with byte value of opCode</returns>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes FromByte(byte
			 opCode)
		{
			if (opCode >= 0 && ((sbyte)opCode) < Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes
				.Values.Length)
			{
				return Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes.Values[opCode];
			}
			return opCode == -1 ? Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogOpCodes.OpInvalid
				 : null;
		}
	}
}
