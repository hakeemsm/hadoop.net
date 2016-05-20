using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>MD5 of MD5 of CRC32.</summary>
	public class MD5MD5CRC32FileChecksum : org.apache.hadoop.fs.FileChecksum
	{
		public const int LENGTH = org.apache.hadoop.io.MD5Hash.MD5_LEN + (int.SIZE + long
			.SIZE) / byte.SIZE;

		private int bytesPerCRC;

		private long crcPerBlock;

		private org.apache.hadoop.io.MD5Hash md5;

		/// <summary>Same as this(0, 0, null)</summary>
		public MD5MD5CRC32FileChecksum()
			: this(0, 0, null)
		{
		}

		/// <summary>Create a MD5FileChecksum</summary>
		public MD5MD5CRC32FileChecksum(int bytesPerCRC, long crcPerBlock, org.apache.hadoop.io.MD5Hash
			 md5)
		{
			this.bytesPerCRC = bytesPerCRC;
			this.crcPerBlock = crcPerBlock;
			this.md5 = md5;
		}

		public override string getAlgorithmName()
		{
			return "MD5-of-" + crcPerBlock + "MD5-of-" + bytesPerCRC + getCrcType().ToString(
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.util.DataChecksum.Type getCrcTypeFromAlgorithmName
			(string algorithm)
		{
			if (algorithm.EndsWith(org.apache.hadoop.util.DataChecksum.Type.CRC32.ToString()))
			{
				return org.apache.hadoop.util.DataChecksum.Type.CRC32;
			}
			else
			{
				if (algorithm.EndsWith(org.apache.hadoop.util.DataChecksum.Type.CRC32C.ToString()
					))
				{
					return org.apache.hadoop.util.DataChecksum.Type.CRC32C;
				}
			}
			throw new System.IO.IOException("Unknown checksum type in " + algorithm);
		}

		public override int getLength()
		{
			return LENGTH;
		}

		public override byte[] getBytes()
		{
			return org.apache.hadoop.io.WritableUtils.toByteArray(this);
		}

		/// <summary>returns the CRC type</summary>
		public virtual org.apache.hadoop.util.DataChecksum.Type getCrcType()
		{
			// default to the one that is understood by all releases.
			return org.apache.hadoop.util.DataChecksum.Type.CRC32;
		}

		public override org.apache.hadoop.fs.Options.ChecksumOpt getChecksumOpt()
		{
			return new org.apache.hadoop.fs.Options.ChecksumOpt(getCrcType(), bytesPerCRC);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			bytesPerCRC = @in.readInt();
			crcPerBlock = @in.readLong();
			md5 = org.apache.hadoop.io.MD5Hash.read(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			@out.writeInt(bytesPerCRC);
			@out.writeLong(crcPerBlock);
			md5.write(@out);
		}

		/// <summary>Write that object to xml output.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void write(org.znerd.xmlenc.XMLOutputter xml, org.apache.hadoop.fs.MD5MD5CRC32FileChecksum
			 that)
		{
			xml.startTag(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.MD5MD5CRC32FileChecksum
				)).getName());
			if (that != null)
			{
				xml.attribute("bytesPerCRC", string.Empty + that.bytesPerCRC);
				xml.attribute("crcPerBlock", string.Empty + that.crcPerBlock);
				xml.attribute("crcType", string.Empty + that.getCrcType().ToString());
				xml.attribute("md5", string.Empty + that.md5);
			}
			xml.endTag();
		}

		/// <summary>Return the object represented in the attributes.</summary>
		/// <exception cref="org.xml.sax.SAXException"/>
		public static org.apache.hadoop.fs.MD5MD5CRC32FileChecksum valueOf(org.xml.sax.Attributes
			 attrs)
		{
			string bytesPerCRC = attrs.getValue("bytesPerCRC");
			string crcPerBlock = attrs.getValue("crcPerBlock");
			string md5 = attrs.getValue("md5");
			string crcType = attrs.getValue("crcType");
			org.apache.hadoop.util.DataChecksum.Type finalCrcType;
			if (bytesPerCRC == null || crcPerBlock == null || md5 == null)
			{
				return null;
			}
			try
			{
				// old versions don't support crcType.
				if (crcType == null || crcType == string.Empty)
				{
					finalCrcType = org.apache.hadoop.util.DataChecksum.Type.CRC32;
				}
				else
				{
					finalCrcType = org.apache.hadoop.util.DataChecksum.Type.valueOf(crcType);
				}
				switch (finalCrcType)
				{
					case org.apache.hadoop.util.DataChecksum.Type.CRC32:
					{
						return new org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum(System.Convert.ToInt32
							(bytesPerCRC), System.Convert.ToInt32(crcPerBlock), new org.apache.hadoop.io.MD5Hash
							(md5));
					}

					case org.apache.hadoop.util.DataChecksum.Type.CRC32C:
					{
						return new org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum(System.Convert.ToInt32
							(bytesPerCRC), System.Convert.ToInt32(crcPerBlock), new org.apache.hadoop.io.MD5Hash
							(md5));
					}

					default:
					{
						// we should never get here since finalCrcType will
						// hold a valid type or we should have got an exception.
						return null;
					}
				}
			}
			catch (System.Exception e)
			{
				throw new org.xml.sax.SAXException("Invalid attributes: bytesPerCRC=" + bytesPerCRC
					 + ", crcPerBlock=" + crcPerBlock + ", crcType=" + crcType + ", md5=" + md5, e);
			}
		}

		public override string ToString()
		{
			return getAlgorithmName() + ":" + md5;
		}
	}
}
