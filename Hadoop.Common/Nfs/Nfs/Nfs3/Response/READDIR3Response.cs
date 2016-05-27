using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>READDIR3 Response</summary>
	public class READDIR3Response : NFS3Response
	{
		private readonly Nfs3FileAttributes postOpDirAttr;

		private readonly long cookieVerf;

		private readonly READDIR3Response.DirList3 dirList;

		public class Entry3
		{
			private readonly long fileId;

			private readonly string name;

			private readonly long cookie;

			public Entry3(long fileId, string name, long cookie)
			{
				this.fileId = fileId;
				this.name = name;
				this.cookie = cookie;
			}

			internal virtual long GetFileId()
			{
				return fileId;
			}

			[VisibleForTesting]
			public virtual string GetName()
			{
				return name;
			}

			internal virtual long GetCookie()
			{
				return cookie;
			}

			internal static READDIR3Response.Entry3 Deserialzie(XDR xdr)
			{
				long fileId = xdr.ReadHyper();
				string name = xdr.ReadString();
				long cookie = xdr.ReadHyper();
				return new READDIR3Response.Entry3(fileId, name, cookie);
			}

			internal virtual void Seralize(XDR xdr)
			{
				xdr.WriteLongAsHyper(GetFileId());
				xdr.WriteString(GetName());
				xdr.WriteLongAsHyper(GetCookie());
			}
		}

		public class DirList3
		{
			internal readonly IList<READDIR3Response.Entry3> entries;

			internal readonly bool eof;

			public DirList3(READDIR3Response.Entry3[] entries, bool eof)
			{
				this.entries = Sharpen.Collections.UnmodifiableList(Arrays.AsList(entries));
				this.eof = eof;
			}

			[VisibleForTesting]
			public virtual IList<READDIR3Response.Entry3> GetEntries()
			{
				return this.entries;
			}
		}

		public READDIR3Response(int status)
			: this(status, new Nfs3FileAttributes())
		{
		}

		public READDIR3Response(int status, Nfs3FileAttributes postOpAttr)
			: this(status, postOpAttr, 0, null)
		{
		}

		public READDIR3Response(int status, Nfs3FileAttributes postOpAttr, long cookieVerf
			, READDIR3Response.DirList3 dirList)
			: base(status)
		{
			this.postOpDirAttr = postOpAttr;
			this.cookieVerf = cookieVerf;
			this.dirList = dirList;
		}

		public virtual Nfs3FileAttributes GetPostOpAttr()
		{
			return postOpDirAttr;
		}

		public virtual long GetCookieVerf()
		{
			return cookieVerf;
		}

		public virtual READDIR3Response.DirList3 GetDirList()
		{
			return dirList;
		}

		public static READDIR3Response Deserialize(XDR xdr)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpDirAttr = Nfs3FileAttributes.Deserialize(xdr);
			long cookieVerf = 0;
			AList<READDIR3Response.Entry3> entries = new AList<READDIR3Response.Entry3>();
			READDIR3Response.DirList3 dirList = null;
			if (status == Nfs3Status.Nfs3Ok)
			{
				cookieVerf = xdr.ReadHyper();
				while (xdr.ReadBoolean())
				{
					READDIR3Response.Entry3 e = READDIR3Response.Entry3.Deserialzie(xdr);
					entries.AddItem(e);
				}
				bool eof = xdr.ReadBoolean();
				READDIR3Response.Entry3[] allEntries = new READDIR3Response.Entry3[entries.Count]
					;
				Sharpen.Collections.ToArray(entries, allEntries);
				dirList = new READDIR3Response.DirList3(allEntries, eof);
			}
			return new READDIR3Response(status, postOpDirAttr, cookieVerf, dirList);
		}

		public override XDR Serialize(XDR xdr, int xid, Verifier verifier)
		{
			base.Serialize(xdr, xid, verifier);
			xdr.WriteBoolean(true);
			// Attributes follow
			postOpDirAttr.Serialize(xdr);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				xdr.WriteLongAsHyper(cookieVerf);
				foreach (READDIR3Response.Entry3 e in dirList.entries)
				{
					xdr.WriteBoolean(true);
					// Value follows
					e.Seralize(xdr);
				}
				xdr.WriteBoolean(false);
				xdr.WriteBoolean(dirList.eof);
			}
			return xdr;
		}
	}
}
