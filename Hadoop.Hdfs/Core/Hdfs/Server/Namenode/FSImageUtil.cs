using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public sealed class FSImageUtil
	{
		public static readonly byte[] MagicHeader = Sharpen.Runtime.GetBytesForString("HDFSIMG1"
			, Charsets.Utf8);

		public const int FileVersion = 1;

		/// <exception cref="System.IO.IOException"/>
		public static bool CheckFileFormat(RandomAccessFile file)
		{
			if (file.Length() < FSImageFormatProtobuf.Loader.MinimumFileLength)
			{
				return false;
			}
			byte[] magic = new byte[MagicHeader.Length];
			file.ReadFully(magic);
			if (!Arrays.Equals(MagicHeader, magic))
			{
				return false;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public static FsImageProto.FileSummary LoadSummary(RandomAccessFile file)
		{
			int FileLengthFieldSize = 4;
			long fileLength = file.Length();
			file.Seek(fileLength - FileLengthFieldSize);
			int summaryLength = file.ReadInt();
			if (summaryLength <= 0)
			{
				throw new IOException("Negative length of the file");
			}
			file.Seek(fileLength - FileLengthFieldSize - summaryLength);
			byte[] summaryBytes = new byte[summaryLength];
			file.ReadFully(summaryBytes);
			FsImageProto.FileSummary summary = FsImageProto.FileSummary.ParseDelimitedFrom(new 
				ByteArrayInputStream(summaryBytes));
			if (summary.GetOndiskVersion() != FileVersion)
			{
				throw new IOException("Unsupported file version " + summary.GetOndiskVersion());
			}
			if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.ProtobufFormat, summary
				.GetLayoutVersion()))
			{
				throw new IOException("Unsupported layout version " + summary.GetLayoutVersion());
			}
			return summary;
		}

		/// <exception cref="System.IO.IOException"/>
		public static InputStream WrapInputStreamForCompression(Configuration conf, string
			 codec, InputStream @in)
		{
			if (codec.IsEmpty())
			{
				return @in;
			}
			FSImageCompression compression = FSImageCompression.CreateCompression(conf, codec
				);
			CompressionCodec imageCodec = compression.GetImageCodec();
			return imageCodec.CreateInputStream(@in);
		}
	}
}
