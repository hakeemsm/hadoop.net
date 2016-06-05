using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>READDIRPLUS3 Response</summary>
	public class READDIRPLUS3Response : NFS3Response
	{
		private Nfs3FileAttributes postOpDirAttr;

		private readonly long cookieVerf;

		private readonly READDIRPLUS3Response.DirListPlus3 dirListPlus;

		public class EntryPlus3
		{
			private readonly long fileId;

			private readonly string name;

			private readonly long cookie;

			private readonly Nfs3FileAttributes nameAttr;

			private readonly FileHandle objFileHandle;

			public EntryPlus3(long fileId, string name, long cookie, Nfs3FileAttributes nameAttr
				, FileHandle objFileHandle)
			{
				this.fileId = fileId;
				this.name = name;
				this.cookie = cookie;
				this.nameAttr = nameAttr;
				this.objFileHandle = objFileHandle;
			}

			[VisibleForTesting]
			public virtual string GetName()
			{
				return name;
			}

			internal static READDIRPLUS3Response.EntryPlus3 Deseralize(XDR xdr)
			{
				long fileId = xdr.ReadHyper();
				string name = xdr.ReadString();
				long cookie = xdr.ReadHyper();
				xdr.ReadBoolean();
				Nfs3FileAttributes nameAttr = Nfs3FileAttributes.Deserialize(xdr);
				FileHandle objFileHandle = new FileHandle();
				objFileHandle.Deserialize(xdr);
				return new READDIRPLUS3Response.EntryPlus3(fileId, name, cookie, nameAttr, objFileHandle
					);
			}

			internal virtual void Seralize(XDR xdr)
			{
				xdr.WriteLongAsHyper(fileId);
				xdr.WriteString(name);
				xdr.WriteLongAsHyper(cookie);
				xdr.WriteBoolean(true);
				nameAttr.Serialize(xdr);
				xdr.WriteBoolean(true);
				objFileHandle.Serialize(xdr);
			}
		}

		public class DirListPlus3
		{
			internal IList<READDIRPLUS3Response.EntryPlus3> entries;

			internal bool eof;

			public DirListPlus3(READDIRPLUS3Response.EntryPlus3[] entries, bool eof)
			{
				this.entries = Collections.UnmodifiableList(Arrays.AsList(entries));
				this.eof = eof;
			}

			[VisibleForTesting]
			public virtual IList<READDIRPLUS3Response.EntryPlus3> GetEntries()
			{
				return entries;
			}

			internal virtual bool GetEof()
			{
				return eof;
			}
		}

		[VisibleForTesting]
		public virtual READDIRPLUS3Response.DirListPlus3 GetDirListPlus()
		{
			return dirListPlus;
		}

		public READDIRPLUS3Response(int status)
			: this(status, null, 0, null)
		{
		}

		public READDIRPLUS3Response(int status, Nfs3FileAttributes postOpDirAttr, long cookieVerf
			, READDIRPLUS3Response.DirListPlus3 dirListPlus)
			: base(status)
		{
			this.postOpDirAttr = postOpDirAttr;
			this.cookieVerf = cookieVerf;
			this.dirListPlus = dirListPlus;
		}

		public static READDIRPLUS3Response Deserialize(XDR xdr)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpDirAttr = Nfs3FileAttributes.Deserialize(xdr);
			long cookieVerf = 0;
			AList<READDIRPLUS3Response.EntryPlus3> entries = new AList<READDIRPLUS3Response.EntryPlus3
				>();
			READDIRPLUS3Response.DirListPlus3 dirList = null;
			if (status == Nfs3Status.Nfs3Ok)
			{
				cookieVerf = xdr.ReadHyper();
				while (xdr.ReadBoolean())
				{
					READDIRPLUS3Response.EntryPlus3 e = READDIRPLUS3Response.EntryPlus3.Deseralize(xdr
						);
					entries.AddItem(e);
				}
				bool eof = xdr.ReadBoolean();
				READDIRPLUS3Response.EntryPlus3[] allEntries = new READDIRPLUS3Response.EntryPlus3
					[entries.Count];
				Collections.ToArray(entries, allEntries);
				dirList = new READDIRPLUS3Response.DirListPlus3(allEntries, eof);
			}
			return new READDIRPLUS3Response(status, postOpDirAttr, cookieVerf, dirList);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			@out.WriteBoolean(true);
			// attributes follow
			if (postOpDirAttr == null)
			{
				postOpDirAttr = new Nfs3FileAttributes();
			}
			postOpDirAttr.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteLongAsHyper(cookieVerf);
				foreach (READDIRPLUS3Response.EntryPlus3 f in dirListPlus.GetEntries())
				{
					@out.WriteBoolean(true);
					// next
					f.Seralize(@out);
				}
				@out.WriteBoolean(false);
				@out.WriteBoolean(dirListPlus.GetEof());
			}
			return @out;
		}
	}
}
