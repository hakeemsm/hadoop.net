using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>SetAttr3 contains the file attributes that can be set from the client.</summary>
	/// <remarks>
	/// SetAttr3 contains the file attributes that can be set from the client. The
	/// fields are the same as the similarly named fields in the NFS3Attributes
	/// structure.
	/// </remarks>
	public class SetAttr3
	{
		public const int TimeDontChange = 0;

		public const int TimeSetToServerTime = 1;

		public const int TimeSetToClientTime = 2;

		private int mode;

		private int uid;

		private int gid;

		private long size;

		private NfsTime atime;

		private NfsTime mtime;

		private EnumSet<SetAttr3.SetAttrField> updateFields;

		public enum SetAttrField
		{
			Mode,
			Uid,
			Gid,
			Size,
			Atime,
			Mtime
		}

		public SetAttr3()
		{
			// Options for time stamp change
			mode = 0;
			uid = 0;
			gid = 0;
			size = 0;
			updateFields = EnumSet.NoneOf<SetAttr3.SetAttrField>();
		}

		public SetAttr3(int mode, int uid, int gid, long size, NfsTime atime, NfsTime mtime
			, EnumSet<SetAttr3.SetAttrField> updateFields)
		{
			this.mode = mode;
			this.uid = uid;
			this.gid = gid;
			this.size = size;
			this.updateFields = updateFields;
		}

		public virtual int GetMode()
		{
			return mode;
		}

		public virtual int GetUid()
		{
			return uid;
		}

		public virtual int GetGid()
		{
			return gid;
		}

		public virtual void SetGid(int gid)
		{
			this.gid = gid;
		}

		public virtual long GetSize()
		{
			return size;
		}

		public virtual NfsTime GetAtime()
		{
			return atime;
		}

		public virtual NfsTime GetMtime()
		{
			return mtime;
		}

		public virtual EnumSet<SetAttr3.SetAttrField> GetUpdateFields()
		{
			return updateFields;
		}

		public virtual void SetUpdateFields(EnumSet<SetAttr3.SetAttrField> updateFields)
		{
			this.updateFields = updateFields;
		}

		public virtual void Serialize(XDR xdr)
		{
			if (!updateFields.Contains(SetAttr3.SetAttrField.Mode))
			{
				xdr.WriteBoolean(false);
			}
			else
			{
				xdr.WriteBoolean(true);
				xdr.WriteInt(mode);
			}
			if (!updateFields.Contains(SetAttr3.SetAttrField.Uid))
			{
				xdr.WriteBoolean(false);
			}
			else
			{
				xdr.WriteBoolean(true);
				xdr.WriteInt(uid);
			}
			if (!updateFields.Contains(SetAttr3.SetAttrField.Gid))
			{
				xdr.WriteBoolean(false);
			}
			else
			{
				xdr.WriteBoolean(true);
				xdr.WriteInt(gid);
			}
			if (!updateFields.Contains(SetAttr3.SetAttrField.Size))
			{
				xdr.WriteBoolean(false);
			}
			else
			{
				xdr.WriteBoolean(true);
				xdr.WriteLongAsHyper(size);
			}
			if (!updateFields.Contains(SetAttr3.SetAttrField.Atime))
			{
				xdr.WriteBoolean(false);
			}
			else
			{
				xdr.WriteBoolean(true);
				atime.Serialize(xdr);
			}
			if (!updateFields.Contains(SetAttr3.SetAttrField.Mtime))
			{
				xdr.WriteBoolean(false);
			}
			else
			{
				xdr.WriteBoolean(true);
				mtime.Serialize(xdr);
			}
		}

		public virtual void Deserialize(XDR xdr)
		{
			if (xdr.ReadBoolean())
			{
				mode = xdr.ReadInt();
				updateFields.AddItem(SetAttr3.SetAttrField.Mode);
			}
			if (xdr.ReadBoolean())
			{
				uid = xdr.ReadInt();
				updateFields.AddItem(SetAttr3.SetAttrField.Uid);
			}
			if (xdr.ReadBoolean())
			{
				gid = xdr.ReadInt();
				updateFields.AddItem(SetAttr3.SetAttrField.Gid);
			}
			if (xdr.ReadBoolean())
			{
				size = xdr.ReadHyper();
				updateFields.AddItem(SetAttr3.SetAttrField.Size);
			}
			int timeSetHow = xdr.ReadInt();
			if (timeSetHow == TimeSetToClientTime)
			{
				atime = NfsTime.Deserialize(xdr);
				updateFields.AddItem(SetAttr3.SetAttrField.Atime);
			}
			else
			{
				if (timeSetHow == TimeSetToServerTime)
				{
					atime = new NfsTime(Runtime.CurrentTimeMillis());
					updateFields.AddItem(SetAttr3.SetAttrField.Atime);
				}
			}
			timeSetHow = xdr.ReadInt();
			if (timeSetHow == TimeSetToClientTime)
			{
				mtime = NfsTime.Deserialize(xdr);
				updateFields.AddItem(SetAttr3.SetAttrField.Mtime);
			}
			else
			{
				if (timeSetHow == TimeSetToServerTime)
				{
					mtime = new NfsTime(Runtime.CurrentTimeMillis());
					updateFields.AddItem(SetAttr3.SetAttrField.Mtime);
				}
			}
		}
	}
}
