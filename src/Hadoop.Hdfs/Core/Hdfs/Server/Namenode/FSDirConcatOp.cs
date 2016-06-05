using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Restrictions for a concat operation:
	/// <pre>
	/// 1.
	/// </summary>
	/// <remarks>
	/// Restrictions for a concat operation:
	/// <pre>
	/// 1. the src file and the target file are in the same dir
	/// 2. all the source files are not in snapshot
	/// 3. any source file cannot be the same with the target file
	/// 4. source files cannot be under construction or empty
	/// 5. source file's preferred block size cannot be greater than the target file
	/// </pre>
	/// </remarks>
	internal class FSDirConcatOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus Concat(FSDirectory fsd, string target, string[] srcs
			, bool logRetryCache)
		{
			Preconditions.CheckArgument(!target.IsEmpty(), "Target file name is empty");
			Preconditions.CheckArgument(srcs != null && srcs.Length > 0, "No sources given");
			System.Diagnostics.Debug.Assert(srcs != null);
			if (FSDirectory.Log.IsDebugEnabled())
			{
				FSDirectory.Log.Debug("concat {} to {}", Arrays.ToString(srcs), target);
			}
			INodesInPath targetIIP = fsd.GetINodesInPath4Write(target);
			// write permission for the target
			FSPermissionChecker pc = null;
			if (fsd.IsPermissionEnabled())
			{
				pc = fsd.GetPermissionChecker();
				fsd.CheckPathAccess(pc, targetIIP, FsAction.Write);
			}
			// check the target
			VerifyTargetFile(fsd, target, targetIIP);
			// check the srcs
			INodeFile[] srcFiles = VerifySrcFiles(fsd, srcs, targetIIP, pc);
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.concat: " + Arrays.ToString(srcs) 
					+ " to " + target);
			}
			long timestamp = Time.Now();
			fsd.WriteLock();
			try
			{
				UnprotectedConcat(fsd, targetIIP, srcFiles, timestamp);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogConcat(target, srcs, timestamp, logRetryCache);
			return fsd.GetAuditFileInfo(targetIIP);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void VerifyTargetFile(FSDirectory fsd, string target, INodesInPath
			 targetIIP)
		{
			// check the target
			if (fsd.GetEZForPath(targetIIP) != null)
			{
				throw new HadoopIllegalArgumentException("concat can not be called for files in an encryption zone."
					);
			}
			INodeFile targetINode = INodeFile.ValueOf(targetIIP.GetLastINode(), target);
			if (targetINode.IsUnderConstruction())
			{
				throw new HadoopIllegalArgumentException("concat: target file " + target + " is under construction"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static INodeFile[] VerifySrcFiles(FSDirectory fsd, string[] srcs, INodesInPath
			 targetIIP, FSPermissionChecker pc)
		{
			// to make sure no two files are the same
			ICollection<INodeFile> si = new LinkedHashSet<INodeFile>();
			INodeFile targetINode = targetIIP.GetLastINode().AsFile();
			INodeDirectory targetParent = targetINode.GetParent();
			// now check the srcs
			foreach (string src in srcs)
			{
				INodesInPath iip = fsd.GetINodesInPath4Write(src);
				// permission check for srcs
				if (pc != null)
				{
					fsd.CheckPathAccess(pc, iip, FsAction.Read);
					// read the file
					fsd.CheckParentAccess(pc, iip, FsAction.Write);
				}
				// for delete
				INode srcINode = iip.GetLastINode();
				INodeFile srcINodeFile = INodeFile.ValueOf(srcINode, src);
				// make sure the src file and the target file are in the same dir
				if (srcINodeFile.GetParent() != targetParent)
				{
					throw new HadoopIllegalArgumentException("Source file " + src + " is not in the same directory with the target "
						 + targetIIP.GetPath());
				}
				// make sure all the source files are not in snapshot
				if (srcINode.IsInLatestSnapshot(iip.GetLatestSnapshotId()))
				{
					throw new SnapshotException("Concat: the source file " + src + " is in snapshot");
				}
				// check if the file has other references.
				if (srcINode.IsReference() && ((INodeReference.WithCount)srcINode.AsReference().GetReferredINode
					()).GetReferenceCount() > 1)
				{
					throw new SnapshotException("Concat: the source file " + src + " is referred by some other reference in some snapshot."
						);
				}
				// source file cannot be the same with the target file
				if (srcINode == targetINode)
				{
					throw new HadoopIllegalArgumentException("concat: the src file " + src + " is the same with the target file "
						 + targetIIP.GetPath());
				}
				// source file cannot be under construction or empty
				if (srcINodeFile.IsUnderConstruction() || srcINodeFile.NumBlocks() == 0)
				{
					throw new HadoopIllegalArgumentException("concat: source file " + src + " is invalid or empty or underConstruction"
						);
				}
				// source file's preferred block size cannot be greater than the target
				// file
				if (srcINodeFile.GetPreferredBlockSize() > targetINode.GetPreferredBlockSize())
				{
					throw new HadoopIllegalArgumentException("concat: source file " + src + " has preferred block size "
						 + srcINodeFile.GetPreferredBlockSize() + " which is greater than the target file's preferred block size "
						 + targetINode.GetPreferredBlockSize());
				}
				si.AddItem(srcINodeFile);
			}
			// make sure no two files are the same
			if (si.Count < srcs.Length)
			{
				// it means at least two files are the same
				throw new HadoopIllegalArgumentException("concat: at least two of the source files are the same"
					);
			}
			return Sharpen.Collections.ToArray(si, new INodeFile[si.Count]);
		}

		private static QuotaCounts ComputeQuotaDeltas(FSDirectory fsd, INodeFile target, 
			INodeFile[] srcList)
		{
			QuotaCounts deltas = new QuotaCounts.Builder().Build();
			short targetRepl = target.GetBlockReplication();
			foreach (INodeFile src in srcList)
			{
				short srcRepl = src.GetBlockReplication();
				long fileSize = src.ComputeFileSize();
				if (targetRepl != srcRepl)
				{
					deltas.AddStorageSpace(fileSize * (targetRepl - srcRepl));
					BlockStoragePolicy bsp = fsd.GetBlockStoragePolicySuite().GetPolicy(src.GetStoragePolicyID
						());
					if (bsp != null)
					{
						IList<StorageType> srcTypeChosen = bsp.ChooseStorageTypes(srcRepl);
						foreach (StorageType t in srcTypeChosen)
						{
							if (t.SupportTypeQuota())
							{
								deltas.AddTypeSpace(t, -fileSize);
							}
						}
						IList<StorageType> targetTypeChosen = bsp.ChooseStorageTypes(targetRepl);
						foreach (StorageType t_1 in targetTypeChosen)
						{
							if (t_1.SupportTypeQuota())
							{
								deltas.AddTypeSpace(t_1, fileSize);
							}
						}
					}
				}
			}
			return deltas;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		private static void VerifyQuota(FSDirectory fsd, INodesInPath targetIIP, QuotaCounts
			 deltas)
		{
			if (!fsd.GetFSNamesystem().IsImageLoaded() || fsd.ShouldSkipQuotaChecks())
			{
				// Do not check quota if editlog is still being processed
				return;
			}
			FSDirectory.VerifyQuota(targetIIP, targetIIP.Length() - 1, deltas, null);
		}

		/// <summary>Concat all the blocks from srcs to trg and delete the srcs files</summary>
		/// <param name="fsd">FSDirectory</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void UnprotectedConcat(FSDirectory fsd, INodesInPath targetIIP, INodeFile
			[] srcList, long timestamp)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* FSNamesystem.concat to " + targetIIP.GetPath(
					));
			}
			INodeFile trgInode = targetIIP.GetLastINode().AsFile();
			QuotaCounts deltas = ComputeQuotaDeltas(fsd, trgInode, srcList);
			VerifyQuota(fsd, targetIIP, deltas);
			// the target file can be included in a snapshot
			trgInode.RecordModification(targetIIP.GetLatestSnapshotId());
			INodeDirectory trgParent = targetIIP.GetINode(-2).AsDirectory();
			trgInode.ConcatBlocks(srcList);
			// since we are in the same dir - we can use same parent to remove files
			int count = 0;
			foreach (INodeFile nodeToRemove in srcList)
			{
				if (nodeToRemove != null)
				{
					nodeToRemove.SetBlocks(null);
					nodeToRemove.GetParent().RemoveChild(nodeToRemove);
					fsd.GetINodeMap().Remove(nodeToRemove);
					count++;
				}
			}
			trgInode.SetModificationTime(timestamp, targetIIP.GetLatestSnapshotId());
			trgParent.UpdateModificationTime(timestamp, targetIIP.GetLatestSnapshotId());
			// update quota on the parent directory with deltas
			FSDirectory.UnprotectedUpdateCount(targetIIP, targetIIP.Length() - 1, deltas);
		}
	}
}
