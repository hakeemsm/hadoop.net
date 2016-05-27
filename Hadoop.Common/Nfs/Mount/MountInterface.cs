using System.Net;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Mount
{
	/// <summary>
	/// This is an interface that should be implemented for handle Mountd related
	/// requests.
	/// </summary>
	/// <remarks>
	/// This is an interface that should be implemented for handle Mountd related
	/// requests. See RFC 1094 for more details.
	/// </remarks>
	public abstract class MountInterface
	{
		/// <summary>Mount procedures</summary>
		[System.Serializable]
		public sealed class MNTPROC
		{
			public static readonly MountInterface.MNTPROC Null = new MountInterface.MNTPROC();

			public static readonly MountInterface.MNTPROC Mnt = new MountInterface.MNTPROC();

			public static readonly MountInterface.MNTPROC Dump = new MountInterface.MNTPROC();

			public static readonly MountInterface.MNTPROC Umnt = new MountInterface.MNTPROC();

			public static readonly MountInterface.MNTPROC Umntall = new MountInterface.MNTPROC
				();

			public static readonly MountInterface.MNTPROC Export = new MountInterface.MNTPROC
				();

			public static readonly MountInterface.MNTPROC Exportall = new MountInterface.MNTPROC
				();

			public static readonly MountInterface.MNTPROC Pathconf = new MountInterface.MNTPROC
				();

			// the order of the values below are significant.
			/// <returns>the int value representing the procedure.</returns>
			public int GetValue()
			{
				return Ordinal();
			}

			/// <returns>the procedure corresponding to the value.</returns>
			public static MountInterface.MNTPROC FromValue(int value)
			{
				if (value < 0 || value >= Values().Length)
				{
					return null;
				}
				return Values()[value];
			}
		}

		/// <summary>MNTPROC_NULL - Do Nothing</summary>
		public abstract XDR NullOp(XDR @out, int xid, IPAddress client);

		/// <summary>MNTPROC_MNT - Add mount entry</summary>
		public abstract XDR Mnt(XDR xdr, XDR @out, int xid, IPAddress client);

		/// <summary>MNTPROC_DUMP - Return mount entries</summary>
		public abstract XDR Dump(XDR @out, int xid, IPAddress client);

		/// <summary>MNTPROC_UMNT - Remove mount entry</summary>
		public abstract XDR Umnt(XDR xdr, XDR @out, int xid, IPAddress client);

		/// <summary>MNTPROC_UMNTALL - Remove all mount entries</summary>
		public abstract XDR Umntall(XDR @out, int xid, IPAddress client);
		//public XDR exportall(XDR out, int xid, InetAddress client);
		//public XDR pathconf(XDR out, int xid, InetAddress client);
	}

	public static class MountInterfaceConstants
	{
	}
}
