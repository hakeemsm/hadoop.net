using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Translates from edit log ops to inotify events.</summary>
	public class InotifyFSEditLogOpTranslator
	{
		private static long GetSize(FSEditLogOp.AddCloseOp acOp)
		{
			long size = 0;
			foreach (Block b in acOp.GetBlocks())
			{
				size += b.GetNumBytes();
			}
			return size;
		}

		public static EventBatch Translate(FSEditLogOp op)
		{
			switch (op.opCode)
			{
				case FSEditLogOpCodes.OpAdd:
				{
					FSEditLogOp.AddOp addOp = (FSEditLogOp.AddOp)op;
					if (addOp.blocks.Length == 0)
					{
						// create
						return new EventBatch(op.txid, new Event[] { new Event.CreateEvent.Builder().Path
							(addOp.path).Ctime(addOp.atime).Replication(addOp.replication).OwnerName(addOp.permissions
							.GetUserName()).GroupName(addOp.permissions.GetGroupName()).Perms(addOp.permissions
							.GetPermission()).Overwrite(addOp.overwrite).DefaultBlockSize(addOp.blockSize).INodeType
							(Event.CreateEvent.INodeType.File).Build() });
					}
					else
					{
						// append
						return new EventBatch(op.txid, new Event[] { new Event.AppendEvent.Builder().Path
							(addOp.path).Build() });
					}
					goto case FSEditLogOpCodes.OpClose;
				}

				case FSEditLogOpCodes.OpClose:
				{
					FSEditLogOp.CloseOp cOp = (FSEditLogOp.CloseOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.CloseEvent(cOp.path, GetSize
						(cOp), cOp.mtime) });
				}

				case FSEditLogOpCodes.OpAppend:
				{
					FSEditLogOp.AppendOp appendOp = (FSEditLogOp.AppendOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.AppendEvent.Builder().Path
						(appendOp.path).NewBlock(appendOp.newBlock).Build() });
				}

				case FSEditLogOpCodes.OpSetReplication:
				{
					FSEditLogOp.SetReplicationOp setRepOp = (FSEditLogOp.SetReplicationOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Replication).Path(setRepOp
						.path).Replication(setRepOp.replication).Build() });
				}

				case FSEditLogOpCodes.OpConcatDelete:
				{
					FSEditLogOp.ConcatDeleteOp cdOp = (FSEditLogOp.ConcatDeleteOp)op;
					IList<Event> events = Lists.NewArrayList();
					events.AddItem(new Event.AppendEvent.Builder().Path(cdOp.trg).Build());
					foreach (string src in cdOp.srcs)
					{
						events.AddItem(new Event.UnlinkEvent.Builder().Path(src).Timestamp(cdOp.timestamp
							).Build());
					}
					events.AddItem(new Event.CloseEvent(cdOp.trg, -1, cdOp.timestamp));
					return new EventBatch(op.txid, Sharpen.Collections.ToArray(events, new Event[0]));
				}

				case FSEditLogOpCodes.OpRenameOld:
				{
					FSEditLogOp.RenameOldOp rnOpOld = (FSEditLogOp.RenameOldOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.RenameEvent.Builder().SrcPath
						(rnOpOld.src).DstPath(rnOpOld.dst).Timestamp(rnOpOld.timestamp).Build() });
				}

				case FSEditLogOpCodes.OpRename:
				{
					FSEditLogOp.RenameOp rnOp = (FSEditLogOp.RenameOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.RenameEvent.Builder().SrcPath
						(rnOp.src).DstPath(rnOp.dst).Timestamp(rnOp.timestamp).Build() });
				}

				case FSEditLogOpCodes.OpDelete:
				{
					FSEditLogOp.DeleteOp delOp = (FSEditLogOp.DeleteOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.UnlinkEvent.Builder().Path
						(delOp.path).Timestamp(delOp.timestamp).Build() });
				}

				case FSEditLogOpCodes.OpMkdir:
				{
					FSEditLogOp.MkdirOp mkOp = (FSEditLogOp.MkdirOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.CreateEvent.Builder().Path
						(mkOp.path).Ctime(mkOp.timestamp).OwnerName(mkOp.permissions.GetUserName()).GroupName
						(mkOp.permissions.GetGroupName()).Perms(mkOp.permissions.GetPermission()).INodeType
						(Event.CreateEvent.INodeType.Directory).Build() });
				}

				case FSEditLogOpCodes.OpSetPermissions:
				{
					FSEditLogOp.SetPermissionsOp permOp = (FSEditLogOp.SetPermissionsOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Perms).Path(permOp.src).Perms
						(permOp.permissions).Build() });
				}

				case FSEditLogOpCodes.OpSetOwner:
				{
					FSEditLogOp.SetOwnerOp ownOp = (FSEditLogOp.SetOwnerOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Owner).Path(ownOp.src).OwnerName
						(ownOp.username).GroupName(ownOp.groupname).Build() });
				}

				case FSEditLogOpCodes.OpTimes:
				{
					FSEditLogOp.TimesOp timesOp = (FSEditLogOp.TimesOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Times).Path(timesOp.path)
						.Atime(timesOp.atime).Mtime(timesOp.mtime).Build() });
				}

				case FSEditLogOpCodes.OpSymlink:
				{
					FSEditLogOp.SymlinkOp symOp = (FSEditLogOp.SymlinkOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.CreateEvent.Builder().Path
						(symOp.path).Ctime(symOp.atime).OwnerName(symOp.permissionStatus.GetUserName()).
						GroupName(symOp.permissionStatus.GetGroupName()).Perms(symOp.permissionStatus.GetPermission
						()).SymlinkTarget(symOp.value).INodeType(Event.CreateEvent.INodeType.Symlink).Build
						() });
				}

				case FSEditLogOpCodes.OpRemoveXattr:
				{
					FSEditLogOp.RemoveXAttrOp rxOp = (FSEditLogOp.RemoveXAttrOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Xattrs).Path(rxOp.src).XAttrs
						(rxOp.xAttrs).XAttrsRemoved(true).Build() });
				}

				case FSEditLogOpCodes.OpSetXattr:
				{
					FSEditLogOp.SetXAttrOp sxOp = (FSEditLogOp.SetXAttrOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Xattrs).Path(sxOp.src).XAttrs
						(sxOp.xAttrs).XAttrsRemoved(false).Build() });
				}

				case FSEditLogOpCodes.OpSetAcl:
				{
					FSEditLogOp.SetAclOp saOp = (FSEditLogOp.SetAclOp)op;
					return new EventBatch(op.txid, new Event[] { new Event.MetadataUpdateEvent.Builder
						().MetadataType(Event.MetadataUpdateEvent.MetadataType.Acls).Path(saOp.src).Acls
						(saOp.aclEntries).Build() });
				}

				default:
				{
					return null;
				}
			}
		}
	}
}
