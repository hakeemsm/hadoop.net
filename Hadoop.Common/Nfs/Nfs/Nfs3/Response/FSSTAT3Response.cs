using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>FSSTAT3 Response</summary>
	public class FSSTAT3Response : NFS3Response
	{
		private Nfs3FileAttributes postOpAttr;

		private readonly long tbytes;

		private readonly long fbytes;

		private readonly long abytes;

		private readonly long tfiles;

		private readonly long ffiles;

		private readonly long afiles;

		private readonly int invarsec;

		public FSSTAT3Response(int status)
			: this(status, null, 0, 0, 0, 0, 0, 0, 0)
		{
		}

		public FSSTAT3Response(int status, Nfs3FileAttributes postOpAttr, long tbytes, long
			 fbytes, long abytes, long tfiles, long ffiles, long afiles, int invarsec)
			: base(status)
		{
			// The total size, in bytes, of the file system.
			// The amount of free space, in bytes, in the file system.
			/*
			* The amount of free space, in bytes, available to the user identified by the
			* authentication information in the RPC. (This reflects space that is
			* reserved by the file system; it does not reflect any quota system
			* implemented by the server.)
			*/
			/*
			* The total number of file slots in the file system. (On a UNIX server, this
			* often corresponds to the number of inodes configured.)
			*/
			/* The number of free file slots in the file system. */
			/*
			* The number of free file slots that are available to the user corresponding
			* to the authentication information in the RPC. (This reflects slots that are
			* reserved by the file system; it does not reflect any quota system
			* implemented by the server.)
			*/
			/*
			* A measure of file system volatility: this is the number of seconds for
			* which the file system is not expected to change. For a volatile, frequently
			* updated file system, this will be 0. For an immutable file system, such as
			* a CD-ROM, this would be the largest unsigned integer. For file systems that
			* are infrequently modified, for example, one containing local executable
			* programs and on-line documentation, a value corresponding to a few hours or
			* days might be used. The client may use this as a hint in tuning its cache
			* management. Note however, this measure is assumed to be dynamic and may
			* change at any time.
			*/
			this.postOpAttr = postOpAttr;
			this.tbytes = tbytes;
			this.fbytes = fbytes;
			this.abytes = abytes;
			this.tfiles = tfiles;
			this.ffiles = ffiles;
			this.afiles = afiles;
			this.invarsec = invarsec;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.FSSTAT3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpAttr = Nfs3FileAttributes.Deserialize(xdr);
			long tbytes = 0;
			long fbytes = 0;
			long abytes = 0;
			long tfiles = 0;
			long ffiles = 0;
			long afiles = 0;
			int invarsec = 0;
			if (status == Nfs3Status.Nfs3Ok)
			{
				tbytes = xdr.ReadHyper();
				fbytes = xdr.ReadHyper();
				abytes = xdr.ReadHyper();
				tfiles = xdr.ReadHyper();
				ffiles = xdr.ReadHyper();
				afiles = xdr.ReadHyper();
				invarsec = xdr.ReadInt();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.FSSTAT3Response(status, postOpAttr
				, tbytes, fbytes, abytes, tfiles, ffiles, afiles, invarsec);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			@out.WriteBoolean(true);
			if (postOpAttr == null)
			{
				postOpAttr = new Nfs3FileAttributes();
			}
			postOpAttr.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteLongAsHyper(tbytes);
				@out.WriteLongAsHyper(fbytes);
				@out.WriteLongAsHyper(abytes);
				@out.WriteLongAsHyper(tfiles);
				@out.WriteLongAsHyper(ffiles);
				@out.WriteLongAsHyper(afiles);
				@out.WriteInt(invarsec);
			}
			return @out;
		}
	}
}
