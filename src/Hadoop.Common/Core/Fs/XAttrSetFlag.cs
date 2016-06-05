using System.IO;
using Org.Apache.Hadoop;


namespace Org.Apache.Hadoop.FS
{
	[System.Serializable]
	public sealed class XAttrSetFlag
	{
		/// <summary>Create a new xattr.</summary>
		/// <remarks>
		/// Create a new xattr.
		/// If the xattr exists already, exception will be thrown.
		/// </remarks>
		public static readonly Org.Apache.Hadoop.FS.XAttrSetFlag Create = new Org.Apache.Hadoop.FS.XAttrSetFlag
			((short)unchecked((int)(0x01)));

		/// <summary>Replace a existing xattr.</summary>
		/// <remarks>
		/// Replace a existing xattr.
		/// If the xattr does not exist, exception will be thrown.
		/// </remarks>
		public static readonly Org.Apache.Hadoop.FS.XAttrSetFlag Replace = new Org.Apache.Hadoop.FS.XAttrSetFlag
			((short)unchecked((int)(0x02)));

		private readonly short flag;

		private XAttrSetFlag(short flag)
		{
			this.flag = flag;
		}

		internal short GetFlag()
		{
			return Org.Apache.Hadoop.FS.XAttrSetFlag.flag;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Validate(string xAttrName, bool xAttrExists, EnumSet<Org.Apache.Hadoop.FS.XAttrSetFlag
			> flag)
		{
			if (flag == null || flag.IsEmpty())
			{
				throw new HadoopIllegalArgumentException("A flag must be specified.");
			}
			if (xAttrExists)
			{
				if (!flag.Contains(Org.Apache.Hadoop.FS.XAttrSetFlag.Replace))
				{
					throw new IOException("XAttr: " + xAttrName + " already exists. The REPLACE flag must be specified."
						);
				}
			}
			else
			{
				if (!flag.Contains(Org.Apache.Hadoop.FS.XAttrSetFlag.Create))
				{
					throw new IOException("XAttr: " + xAttrName + " does not exist. The CREATE flag must be specified."
						);
				}
			}
		}
	}
}
