using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Xml.Sax;
using Org.Znerd.Xmlenc;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>MD5 of MD5 of CRC32.</summary>
	public class MD5MD5CRC32FileChecksum : FileChecksum
	{
		public const int Length = MD5Hash.Md5Len + (int.Size + long.Size) / byte.Size;

		private int bytesPerCRC;

		private long crcPerBlock;

		private MD5Hash md5;

		/// <summary>Same as this(0, 0, null)</summary>
		public MD5MD5CRC32FileChecksum()
			: this(0, 0, null)
		{
		}

		/// <summary>Create a MD5FileChecksum</summary>
		public MD5MD5CRC32FileChecksum(int bytesPerCRC, long crcPerBlock, MD5Hash md5)
		{
			this.bytesPerCRC = bytesPerCRC;
			this.crcPerBlock = crcPerBlock;
			this.md5 = md5;
		}

		public override string GetAlgorithmName()
		{
			return "MD5-of-" + crcPerBlock + "MD5-of-" + bytesPerCRC + GetCrcType().ToString(
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public static DataChecksum.Type GetCrcTypeFromAlgorithmName(string algorithm)
		{
			if (algorithm.EndsWith(DataChecksum.Type.Crc32.ToString()))
			{
				return DataChecksum.Type.Crc32;
			}
			else
			{
				if (algorithm.EndsWith(DataChecksum.Type.Crc32c.ToString()))
				{
					return DataChecksum.Type.Crc32c;
				}
			}
			throw new IOException("Unknown checksum type in " + algorithm);
		}

		public override int GetLength()
		{
			return Length;
		}

		public override byte[] GetBytes()
		{
			return WritableUtils.ToByteArray(this);
		}

		/// <summary>returns the CRC type</summary>
		public virtual DataChecksum.Type GetCrcType()
		{
			// default to the one that is understood by all releases.
			return DataChecksum.Type.Crc32;
		}

		public override Options.ChecksumOpt GetChecksumOpt()
		{
			return new Options.ChecksumOpt(GetCrcType(), bytesPerCRC);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(BinaryReader @in)
		{
			bytesPerCRC = @in.ReadInt();
			crcPerBlock = @in.ReadLong();
			md5 = MD5Hash.Read(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			@out.WriteInt(bytesPerCRC);
			@out.WriteLong(crcPerBlock);
			md5.Write(@out);
		}

		/// <summary>Write that object to xml output.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Write(XMLOutputter xml, Org.Apache.Hadoop.FS.MD5MD5CRC32FileChecksum
			 that)
		{
			xml.StartTag(typeof(Org.Apache.Hadoop.FS.MD5MD5CRC32FileChecksum).FullName);
			if (that != null)
			{
				xml.Attribute("bytesPerCRC", string.Empty + that.bytesPerCRC);
				xml.Attribute("crcPerBlock", string.Empty + that.crcPerBlock);
				xml.Attribute("crcType", string.Empty + that.GetCrcType().ToString());
				xml.Attribute("md5", string.Empty + that.md5);
			}
			xml.EndTag();
		}

		/// <summary>Return the object represented in the attributes.</summary>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static Org.Apache.Hadoop.FS.MD5MD5CRC32FileChecksum ValueOf(Attributes attrs
			)
		{
			string bytesPerCRC = attrs.GetValue("bytesPerCRC");
			string crcPerBlock = attrs.GetValue("crcPerBlock");
			string md5 = attrs.GetValue("md5");
			string crcType = attrs.GetValue("crcType");
			DataChecksum.Type finalCrcType;
			if (bytesPerCRC == null || crcPerBlock == null || md5 == null)
			{
				return null;
			}
			try
			{
				// old versions don't support crcType.
				if (crcType == null || crcType == string.Empty)
				{
					finalCrcType = DataChecksum.Type.Crc32;
				}
				else
				{
					finalCrcType = DataChecksum.Type.ValueOf(crcType);
				}
				switch (finalCrcType)
				{
					case DataChecksum.Type.Crc32:
					{
						return new MD5MD5CRC32GzipFileChecksum(System.Convert.ToInt32(bytesPerCRC), System.Convert.ToInt32
							(crcPerBlock), new MD5Hash(md5));
					}

					case DataChecksum.Type.Crc32c:
					{
						return new MD5MD5CRC32CastagnoliFileChecksum(System.Convert.ToInt32(bytesPerCRC), 
							System.Convert.ToInt32(crcPerBlock), new MD5Hash(md5));
					}

					default:
					{
						// we should never get here since finalCrcType will
						// hold a valid type or we should have got an exception.
						return null;
					}
				}
			}
			catch (Exception e)
			{
				throw new SAXException("Invalid attributes: bytesPerCRC=" + bytesPerCRC + ", crcPerBlock="
					 + crcPerBlock + ", crcType=" + crcType + ", md5=" + md5, e);
			}
		}

		public override string ToString()
		{
			return GetAlgorithmName() + ":" + md5;
		}
	}
}
