using System.Net;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Credential used by AUTH_SYS</summary>
	public class CredentialsSys : Credentials
	{
		private static readonly string Hostname;

		static CredentialsSys()
		{
			try
			{
				string s = Sharpen.Runtime.GetLocalHost().GetHostName();
				Hostname = s;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("HOSTNAME = " + Hostname);
				}
			}
			catch (UnknownHostException e)
			{
				Log.Error("Error setting HOSTNAME", e);
				throw new RuntimeException(e);
			}
		}

		protected internal int mUID;

		protected internal int mGID;

		protected internal int[] mAuxGIDs;

		protected internal string mHostName;

		protected internal int mStamp;

		public CredentialsSys()
			: base(RpcAuthInfo.AuthFlavor.AuthSys)
		{
			this.mCredentialsLength = 0;
			this.mHostName = Hostname;
		}

		public virtual int GetGID()
		{
			return mGID;
		}

		public virtual int GetUID()
		{
			return mUID;
		}

		public virtual int[] GetAuxGIDs()
		{
			return mAuxGIDs;
		}

		public virtual void SetGID(int gid)
		{
			this.mGID = gid;
		}

		public virtual void SetUID(int uid)
		{
			this.mUID = uid;
		}

		public virtual void SetStamp(int stamp)
		{
			this.mStamp = stamp;
		}

		public override void Read(XDR xdr)
		{
			mCredentialsLength = xdr.ReadInt();
			mStamp = xdr.ReadInt();
			mHostName = xdr.ReadString();
			mUID = xdr.ReadInt();
			mGID = xdr.ReadInt();
			int length = xdr.ReadInt();
			mAuxGIDs = new int[length];
			for (int i = 0; i < length; i++)
			{
				mAuxGIDs[i] = xdr.ReadInt();
			}
		}

		public override void Write(XDR xdr)
		{
			// mStamp + mHostName.length + mHostName + mUID + mGID + mAuxGIDs.count
			mCredentialsLength = 20 + Sharpen.Runtime.GetBytesForString(mHostName, Charsets.Utf8
				).Length;
			// mAuxGIDs
			if (mAuxGIDs != null && mAuxGIDs.Length > 0)
			{
				mCredentialsLength += mAuxGIDs.Length * 4;
			}
			xdr.WriteInt(mCredentialsLength);
			xdr.WriteInt(mStamp);
			xdr.WriteString(mHostName);
			xdr.WriteInt(mUID);
			xdr.WriteInt(mGID);
			if ((mAuxGIDs == null) || (mAuxGIDs.Length == 0))
			{
				xdr.WriteInt(0);
			}
			else
			{
				xdr.WriteInt(mAuxGIDs.Length);
				for (int i = 0; i < mAuxGIDs.Length; i++)
				{
					xdr.WriteInt(mAuxGIDs[i]);
				}
			}
		}
	}
}
