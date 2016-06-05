using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	/// <summary>File attrbutes reported in NFS.</summary>
	public class Nfs3FileAttributes
	{
		private int type;

		private int mode;

		private int nlink;

		private int uid;

		private int gid;

		private long size;

		private long used;

		private Nfs3FileAttributes.Specdata3 rdev;

		private long fsid;

		private long fileId;

		private NfsTime atime;

		private NfsTime mtime;

		private NfsTime ctime;

		public class Specdata3
		{
			internal readonly int specdata1;

			internal readonly int specdata2;

			public Specdata3()
			{
				/*
				* The interpretation of the two words depends on the type of file system
				* object. For a block special (NF3BLK) or character special (NF3CHR) file,
				* specdata1 and specdata2 are the major and minor device numbers,
				* respectively. (This is obviously a UNIX-specific interpretation.) For all
				* other file types, these two elements should either be set to 0 or the
				* values should be agreed upon by the client and server. If the client and
				* server do not agree upon the values, the client should treat these fields
				* as if they are set to 0.
				*/
				specdata1 = 0;
				specdata2 = 0;
			}

			public Specdata3(int specdata1, int specdata2)
			{
				this.specdata1 = specdata1;
				this.specdata2 = specdata2;
			}

			public virtual int GetSpecdata1()
			{
				return specdata1;
			}

			public virtual int GetSpecdata2()
			{
				return specdata2;
			}

			public override string ToString()
			{
				return "(Specdata3: specdata1" + specdata1 + ", specdata2:" + specdata2 + ")";
			}
		}

		public Nfs3FileAttributes()
			: this(NfsFileType.Nfsreg, 1, (short)0, 0, 0, 0, 0, 0, 0, 0, new Nfs3FileAttributes.Specdata3
				())
		{
		}

		public Nfs3FileAttributes(NfsFileType nfsType, int nlink, short mode, int uid, int
			 gid, long size, long fsid, long fileId, long mtime, long atime, Nfs3FileAttributes.Specdata3
			 rdev)
		{
			this.type = nfsType.ToValue();
			this.mode = mode;
			this.nlink = nlink;
			this.uid = uid;
			this.gid = gid;
			this.size = size;
			this.used = this.size;
			this.rdev = new Nfs3FileAttributes.Specdata3();
			this.fsid = fsid;
			this.fileId = fileId;
			this.mtime = new NfsTime(mtime);
			this.atime = atime != 0 ? new NfsTime(atime) : this.mtime;
			this.ctime = this.mtime;
			this.rdev = rdev;
		}

		public Nfs3FileAttributes(Nfs3FileAttributes other)
		{
			this.type = other.GetType();
			this.mode = other.GetMode();
			this.nlink = other.GetNlink();
			this.uid = other.GetUid();
			this.gid = other.GetGid();
			this.size = other.GetSize();
			this.used = other.GetUsed();
			this.rdev = new Nfs3FileAttributes.Specdata3();
			this.fsid = other.GetFsid();
			this.fileId = other.GetFileId();
			this.mtime = new NfsTime(other.GetMtime());
			this.atime = new NfsTime(other.GetAtime());
			this.ctime = new NfsTime(other.GetCtime());
		}

		public virtual void Serialize(XDR xdr)
		{
			xdr.WriteInt(type);
			xdr.WriteInt(mode);
			xdr.WriteInt(nlink);
			xdr.WriteInt(uid);
			xdr.WriteInt(gid);
			xdr.WriteLongAsHyper(size);
			xdr.WriteLongAsHyper(used);
			xdr.WriteInt(rdev.GetSpecdata1());
			xdr.WriteInt(rdev.GetSpecdata2());
			xdr.WriteLongAsHyper(fsid);
			xdr.WriteLongAsHyper(fileId);
			atime.Serialize(xdr);
			mtime.Serialize(xdr);
			ctime.Serialize(xdr);
		}

		public static Nfs3FileAttributes Deserialize(XDR xdr)
		{
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			attr.type = xdr.ReadInt();
			attr.mode = xdr.ReadInt();
			attr.nlink = xdr.ReadInt();
			attr.uid = xdr.ReadInt();
			attr.gid = xdr.ReadInt();
			attr.size = xdr.ReadHyper();
			attr.used = xdr.ReadHyper();
			attr.rdev = new Nfs3FileAttributes.Specdata3(xdr.ReadInt(), xdr.ReadInt());
			attr.fsid = xdr.ReadHyper();
			attr.fileId = xdr.ReadHyper();
			attr.atime = NfsTime.Deserialize(xdr);
			attr.mtime = NfsTime.Deserialize(xdr);
			attr.ctime = NfsTime.Deserialize(xdr);
			return attr;
		}

		public override string ToString()
		{
			return string.Format("type:%d, mode:%d, nlink:%d, uid:%d, gid:%d, " + "size:%d, used:%d, rdev:%s, fsid:%d, fileid:%d, atime:%s, "
				 + "mtime:%s, ctime:%s", type, mode, nlink, uid, gid, size, used, rdev, fsid, fileId
				, atime, mtime, ctime);
		}

		public virtual int GetNlink()
		{
			return nlink;
		}

		public virtual long GetUsed()
		{
			return used;
		}

		public virtual long GetFsid()
		{
			return fsid;
		}

		public virtual long GetFileId()
		{
			return fileId;
		}

		public virtual NfsTime GetAtime()
		{
			return atime;
		}

		public virtual NfsTime GetMtime()
		{
			return mtime;
		}

		public virtual NfsTime GetCtime()
		{
			return ctime;
		}

		public virtual int GetType()
		{
			return type;
		}

		public virtual WccAttr GetWccAttr()
		{
			return new WccAttr(size, mtime, ctime);
		}

		public virtual long GetSize()
		{
			return size;
		}

		public virtual void SetSize(long size)
		{
			this.size = size;
		}

		public virtual void SetUsed(long used)
		{
			this.used = used;
		}

		public virtual int GetMode()
		{
			return this.mode;
		}

		public virtual int GetUid()
		{
			return this.uid;
		}

		public virtual int GetGid()
		{
			return this.gid;
		}

		public virtual Nfs3FileAttributes.Specdata3 GetRdev()
		{
			return rdev;
		}

		public virtual void SetRdev(Nfs3FileAttributes.Specdata3 rdev)
		{
			this.rdev = rdev;
		}
	}
}
