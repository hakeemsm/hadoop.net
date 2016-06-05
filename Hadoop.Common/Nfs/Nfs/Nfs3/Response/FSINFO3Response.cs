using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>FSINFO3 Response</summary>
	public class FSINFO3Response : NFS3Response
	{
		private readonly Nfs3FileAttributes postOpAttr;

		private readonly int rtmax;

		private readonly int rtpref;

		private readonly int rtmult;

		private readonly int wtmax;

		private readonly int wtpref;

		private readonly int wtmult;

		private readonly int dtpref;

		private readonly long maxFileSize;

		private readonly NfsTime timeDelta;

		private readonly int properties;

		public FSINFO3Response(int status)
			: this(status, new Nfs3FileAttributes(), 0, 0, 0, 0, 0, 0, 0, 0, null, 0)
		{
		}

		public FSINFO3Response(int status, Nfs3FileAttributes postOpAttr, int rtmax, int 
			rtpref, int rtmult, int wtmax, int wtpref, int wtmult, int dtpref, long maxFileSize
			, NfsTime timeDelta, int properties)
			: base(status)
		{
			/*
			* The maximum size in bytes of a READ request supported by the server. Any
			* READ with a number greater than rtmax will result in a short read of rtmax
			* bytes or less.
			*/
			/*
			* The preferred size of a READ request. This should be the same as rtmax
			* unless there is a clear benefit in performance or efficiency.
			*/
			/* The suggested multiple for the size of a READ request. */
			/*
			* The maximum size of a WRITE request supported by the server. In general,
			* the client is limited by wtmax since there is no guarantee that a server
			* can handle a larger write. Any WRITE with a count greater than wtmax will
			* result in a short write of at most wtmax bytes.
			*/
			/*
			* The preferred size of a WRITE request. This should be the same as wtmax
			* unless there is a clear benefit in performance or efficiency.
			*/
			/*
			* The suggested multiple for the size of a WRITE request.
			*/
			/* The preferred size of a READDIR request. */
			/* The maximum size of a file on the file system. */
			/*
			* The server time granularity. When setting a file time using SETATTR, the
			* server guarantees only to preserve times to this accuracy. If this is {0,
			* 1}, the server can support nanosecond times, {0, 1000000} denotes
			* millisecond precision, and {1, 0} indicates that times are accurate only to
			* the nearest second.
			*/
			/*
			* A bit mask of file system properties. The following values are defined:
			*
			* FSF_LINK If this bit is 1 (TRUE), the file system supports hard links.
			*
			* FSF_SYMLINK If this bit is 1 (TRUE), the file system supports symbolic
			* links.
			*
			* FSF_HOMOGENEOUS If this bit is 1 (TRUE), the information returned by
			* PATHCONF is identical for every file and directory in the file system. If
			* it is 0 (FALSE), the client should retrieve PATHCONF information for each
			* file and directory as required.
			*
			* FSF_CANSETTIME If this bit is 1 (TRUE), the server will set the times for a
			* file via SETATTR if requested (to the accuracy indicated by time_delta). If
			* it is 0 (FALSE), the server cannot set times as requested.
			*/
			this.postOpAttr = postOpAttr;
			this.rtmax = rtmax;
			this.rtpref = rtpref;
			this.rtmult = rtmult;
			this.wtmax = wtmax;
			this.wtpref = wtpref;
			this.wtmult = wtmult;
			this.dtpref = dtpref;
			this.maxFileSize = maxFileSize;
			this.timeDelta = timeDelta;
			this.properties = properties;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.FSINFO3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpObjAttr = Nfs3FileAttributes.Deserialize(xdr);
			int rtmax = 0;
			int rtpref = 0;
			int rtmult = 0;
			int wtmax = 0;
			int wtpref = 0;
			int wtmult = 0;
			int dtpref = 0;
			long maxFileSize = 0;
			NfsTime timeDelta = null;
			int properties = 0;
			if (status == Nfs3Status.Nfs3Ok)
			{
				rtmax = xdr.ReadInt();
				rtpref = xdr.ReadInt();
				rtmult = xdr.ReadInt();
				wtmax = xdr.ReadInt();
				wtpref = xdr.ReadInt();
				wtmult = xdr.ReadInt();
				dtpref = xdr.ReadInt();
				maxFileSize = xdr.ReadHyper();
				timeDelta = NfsTime.Deserialize(xdr);
				properties = xdr.ReadInt();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.FSINFO3Response(status, postOpObjAttr
				, rtmax, rtpref, rtmult, wtmax, wtpref, wtmult, dtpref, maxFileSize, timeDelta, 
				properties);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			@out.WriteBoolean(true);
			postOpAttr.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteInt(rtmax);
				@out.WriteInt(rtpref);
				@out.WriteInt(rtmult);
				@out.WriteInt(wtmax);
				@out.WriteInt(wtpref);
				@out.WriteInt(wtmult);
				@out.WriteInt(dtpref);
				@out.WriteLongAsHyper(maxFileSize);
				timeDelta.Serialize(@out);
				@out.WriteInt(properties);
			}
			return @out;
		}
	}
}
