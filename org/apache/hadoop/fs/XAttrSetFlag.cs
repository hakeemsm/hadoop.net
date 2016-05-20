using Sharpen;

namespace org.apache.hadoop.fs
{
	[System.Serializable]
	public sealed class XAttrSetFlag
	{
		/// <summary>Create a new xattr.</summary>
		/// <remarks>
		/// Create a new xattr.
		/// If the xattr exists already, exception will be thrown.
		/// </remarks>
		public static readonly org.apache.hadoop.fs.XAttrSetFlag CREATE = new org.apache.hadoop.fs.XAttrSetFlag
			((short)unchecked((int)(0x01)));

		/// <summary>Replace a existing xattr.</summary>
		/// <remarks>
		/// Replace a existing xattr.
		/// If the xattr does not exist, exception will be thrown.
		/// </remarks>
		public static readonly org.apache.hadoop.fs.XAttrSetFlag REPLACE = new org.apache.hadoop.fs.XAttrSetFlag
			((short)unchecked((int)(0x02)));

		private readonly short flag;

		private XAttrSetFlag(short flag)
		{
			this.flag = flag;
		}

		internal short getFlag()
		{
			return org.apache.hadoop.fs.XAttrSetFlag.flag;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void validate(string xAttrName, bool xAttrExists, java.util.EnumSet
			<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			if (flag == null || flag.isEmpty())
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("A flag must be specified."
					);
			}
			if (xAttrExists)
			{
				if (!flag.contains(org.apache.hadoop.fs.XAttrSetFlag.REPLACE))
				{
					throw new System.IO.IOException("XAttr: " + xAttrName + " already exists. The REPLACE flag must be specified."
						);
				}
			}
			else
			{
				if (!flag.contains(org.apache.hadoop.fs.XAttrSetFlag.CREATE))
				{
					throw new System.IO.IOException("XAttr: " + xAttrName + " does not exist. The CREATE flag must be specified."
						);
				}
			}
		}
	}
}
