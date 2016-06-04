using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>Dumping the information of a TFile.</summary>
	internal class TFileDumper
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.File.Tfile.TFileDumper
			));

		private TFileDumper()
		{
		}

		[System.Serializable]
		private sealed class Align
		{
			public static readonly TFileDumper.Align Left = new TFileDumper.Align();

			public static readonly TFileDumper.Align Center = new TFileDumper.Align();

			public static readonly TFileDumper.Align Right = new TFileDumper.Align();

			public static readonly TFileDumper.Align ZeroPadded = new TFileDumper.Align();

			// namespace object not constructable.
			internal static string Format(string s, int width, TFileDumper.Align align)
			{
				if (s.Length >= width)
				{
					return s;
				}
				int room = width - s.Length;
				TFileDumper.Align alignAdjusted = align;
				if (room == 1)
				{
					alignAdjusted = TFileDumper.Align.Left;
				}
				if (alignAdjusted == TFileDumper.Align.Left)
				{
					return s + string.Format("%" + room + "s", string.Empty);
				}
				if (alignAdjusted == TFileDumper.Align.Right)
				{
					return string.Format("%" + room + "s", string.Empty) + s;
				}
				if (alignAdjusted == TFileDumper.Align.Center)
				{
					int half = room / 2;
					return string.Format("%" + half + "s", string.Empty) + s + string.Format("%" + (room
						 - half) + "s", string.Empty);
				}
				throw new ArgumentException("Unsupported alignment");
			}

			internal static string Format(long l, int width, TFileDumper.Align align)
			{
				if (align == TFileDumper.Align.ZeroPadded)
				{
					return string.Format("%0" + width + "d", l);
				}
				return Format(System.Convert.ToString(l), width, align);
			}

			internal static int CalculateWidth(string caption, long max)
			{
				return Math.Max(caption.Length, System.Convert.ToString(max).Length);
			}
		}

		/// <summary>Dump information about TFile.</summary>
		/// <param name="file">Path string of the TFile</param>
		/// <param name="out">PrintStream to output the information.</param>
		/// <param name="conf">The configuration object.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void DumpInfo(string file, TextWriter @out, Configuration conf)
		{
			int maxKeySampleLen = 16;
			Path path = new Path(file);
			FileSystem fs = path.GetFileSystem(conf);
			long length = fs.GetFileStatus(path).GetLen();
			FSDataInputStream fsdis = fs.Open(path);
			TFile.Reader reader = new TFile.Reader(fsdis, length, conf);
			try
			{
				LinkedHashMap<string, string> properties = new LinkedHashMap<string, string>();
				int blockCnt = reader.readerBCF.GetBlockCount();
				int metaBlkCnt = reader.readerBCF.metaIndex.index.Count;
				properties["BCFile Version"] = reader.readerBCF.version.ToString();
				properties["TFile Version"] = reader.tfileMeta.version.ToString();
				properties["File Length"] = System.Convert.ToString(length);
				properties["Data Compression"] = reader.readerBCF.GetDefaultCompressionName();
				properties["Record Count"] = System.Convert.ToString(reader.GetEntryCount());
				properties["Sorted"] = bool.ToString(reader.IsSorted());
				if (reader.IsSorted())
				{
					properties["Comparator"] = reader.GetComparatorName();
				}
				properties["Data Block Count"] = Sharpen.Extensions.ToString(blockCnt);
				long dataSize = 0;
				long dataSizeUncompressed = 0;
				if (blockCnt > 0)
				{
					for (int i = 0; i < blockCnt; ++i)
					{
						BCFile.BlockRegion region = reader.readerBCF.dataIndex.GetBlockRegionList()[i];
						dataSize += region.GetCompressedSize();
						dataSizeUncompressed += region.GetRawSize();
					}
					properties["Data Block Bytes"] = System.Convert.ToString(dataSize);
					if (reader.readerBCF.GetDefaultCompressionName() != "none")
					{
						properties["Data Block Uncompressed Bytes"] = System.Convert.ToString(dataSizeUncompressed
							);
						properties["Data Block Compression Ratio"] = string.Format("1:%.1f", (double)dataSizeUncompressed
							 / dataSize);
					}
				}
				properties["Meta Block Count"] = Sharpen.Extensions.ToString(metaBlkCnt);
				long metaSize = 0;
				long metaSizeUncompressed = 0;
				if (metaBlkCnt > 0)
				{
					ICollection<BCFile.MetaIndexEntry> metaBlks = reader.readerBCF.metaIndex.index.Values;
					bool calculateCompression = false;
					for (IEnumerator<BCFile.MetaIndexEntry> it = metaBlks.GetEnumerator(); it.HasNext
						(); )
					{
						BCFile.MetaIndexEntry e = it.Next();
						metaSize += e.GetRegion().GetCompressedSize();
						metaSizeUncompressed += e.GetRegion().GetRawSize();
						if (e.GetCompressionAlgorithm() != Compression.Algorithm.None)
						{
							calculateCompression = true;
						}
					}
					properties["Meta Block Bytes"] = System.Convert.ToString(metaSize);
					if (calculateCompression)
					{
						properties["Meta Block Uncompressed Bytes"] = System.Convert.ToString(metaSizeUncompressed
							);
						properties["Meta Block Compression Ratio"] = string.Format("1:%.1f", (double)metaSizeUncompressed
							 / metaSize);
					}
				}
				properties["Meta-Data Size Ratio"] = string.Format("1:%.1f", (double)dataSize / metaSize
					);
				long leftOverBytes = length - dataSize - metaSize;
				long miscSize = BCFile.Magic.Size() * 2 + long.Size / byte.Size + Utils.Version.Size
					();
				long metaIndexSize = leftOverBytes - miscSize;
				properties["Meta Block Index Bytes"] = System.Convert.ToString(metaIndexSize);
				properties["Headers Etc Bytes"] = System.Convert.ToString(miscSize);
				// Now output the properties table.
				int maxKeyLength = 0;
				ICollection<KeyValuePair<string, string>> entrySet = properties;
				for (IEnumerator<KeyValuePair<string, string>> it_1 = entrySet.GetEnumerator(); it_1
					.HasNext(); )
				{
					KeyValuePair<string, string> e = it_1.Next();
					if (e.Key.Length > maxKeyLength)
					{
						maxKeyLength = e.Key.Length;
					}
				}
				for (IEnumerator<KeyValuePair<string, string>> it_2 = entrySet.GetEnumerator(); it_2
					.HasNext(); )
				{
					KeyValuePair<string, string> e = it_2.Next();
					@out.Printf("%s : %s%n", TFileDumper.Align.Format(e.Key, maxKeyLength, TFileDumper.Align
						.Left), e.Value);
				}
				@out.WriteLine();
				reader.CheckTFileDataIndex();
				if (blockCnt > 0)
				{
					string blkID = "Data-Block";
					int blkIDWidth = TFileDumper.Align.CalculateWidth(blkID, blockCnt);
					int blkIDWidth2 = TFileDumper.Align.CalculateWidth(string.Empty, blockCnt);
					string offset = "Offset";
					int offsetWidth = TFileDumper.Align.CalculateWidth(offset, length);
					string blkLen = "Length";
					int blkLenWidth = TFileDumper.Align.CalculateWidth(blkLen, dataSize / blockCnt * 
						10);
					string rawSize = "Raw-Size";
					int rawSizeWidth = TFileDumper.Align.CalculateWidth(rawSize, dataSizeUncompressed
						 / blockCnt * 10);
					string records = "Records";
					int recordsWidth = TFileDumper.Align.CalculateWidth(records, reader.GetEntryCount
						() / blockCnt * 10);
					string endKey = "End-Key";
					int endKeyWidth = Math.Max(endKey.Length, maxKeySampleLen * 2 + 5);
					@out.Printf("%s %s %s %s %s %s%n", TFileDumper.Align.Format(blkID, blkIDWidth, TFileDumper.Align
						.Center), TFileDumper.Align.Format(offset, offsetWidth, TFileDumper.Align.Center
						), TFileDumper.Align.Format(blkLen, blkLenWidth, TFileDumper.Align.Center), TFileDumper.Align
						.Format(rawSize, rawSizeWidth, TFileDumper.Align.Center), TFileDumper.Align.Format
						(records, recordsWidth, TFileDumper.Align.Center), TFileDumper.Align.Format(endKey
						, endKeyWidth, TFileDumper.Align.Left));
					for (int i = 0; i < blockCnt; ++i)
					{
						BCFile.BlockRegion region = reader.readerBCF.dataIndex.GetBlockRegionList()[i];
						TFile.TFileIndexEntry indexEntry = reader.tfileIndex.GetEntry(i);
						@out.Printf("%s %s %s %s %s ", TFileDumper.Align.Format(TFileDumper.Align.Format(
							i, blkIDWidth2, TFileDumper.Align.ZeroPadded), blkIDWidth, TFileDumper.Align.Left
							), TFileDumper.Align.Format(region.GetOffset(), offsetWidth, TFileDumper.Align.Left
							), TFileDumper.Align.Format(region.GetCompressedSize(), blkLenWidth, TFileDumper.Align
							.Left), TFileDumper.Align.Format(region.GetRawSize(), rawSizeWidth, TFileDumper.Align
							.Left), TFileDumper.Align.Format(indexEntry.kvEntries, recordsWidth, TFileDumper.Align
							.Left));
						byte[] key = indexEntry.key;
						bool asAscii = true;
						int sampleLen = Math.Min(maxKeySampleLen, key.Length);
						for (int j = 0; j < sampleLen; ++j)
						{
							byte b = key[j];
							if ((((sbyte)b) < 32 && b != 9) || (b == 127))
							{
								asAscii = false;
							}
						}
						if (!asAscii)
						{
							@out.Write("0X");
							for (int j_1 = 0; j_1 < sampleLen; ++j_1)
							{
								byte b = key[i];
								@out.Printf("%X", b);
							}
						}
						else
						{
							@out.Write(new string(key, 0, sampleLen, Charsets.Utf8));
						}
						if (sampleLen < key.Length)
						{
							@out.Write("...");
						}
						@out.WriteLine();
					}
				}
				@out.WriteLine();
				if (metaBlkCnt > 0)
				{
					string name = "Meta-Block";
					int maxNameLen = 0;
					ICollection<KeyValuePair<string, BCFile.MetaIndexEntry>> metaBlkEntrySet = reader
						.readerBCF.metaIndex.index;
					for (IEnumerator<KeyValuePair<string, BCFile.MetaIndexEntry>> it = metaBlkEntrySet
						.GetEnumerator(); it_2.HasNext(); )
					{
						KeyValuePair<string, BCFile.MetaIndexEntry> e = it_2.Next();
						if (e.Key.Length > maxNameLen)
						{
							maxNameLen = e.Key.Length;
						}
					}
					int nameWidth = Math.Max(name.Length, maxNameLen);
					string offset = "Offset";
					int offsetWidth = TFileDumper.Align.CalculateWidth(offset, length);
					string blkLen = "Length";
					int blkLenWidth = TFileDumper.Align.CalculateWidth(blkLen, metaSize / metaBlkCnt 
						* 10);
					string rawSize = "Raw-Size";
					int rawSizeWidth = TFileDumper.Align.CalculateWidth(rawSize, metaSizeUncompressed
						 / metaBlkCnt * 10);
					string compression = "Compression";
					int compressionWidth = compression.Length;
					@out.Printf("%s %s %s %s %s%n", TFileDumper.Align.Format(name, nameWidth, TFileDumper.Align
						.Center), TFileDumper.Align.Format(offset, offsetWidth, TFileDumper.Align.Center
						), TFileDumper.Align.Format(blkLen, blkLenWidth, TFileDumper.Align.Center), TFileDumper.Align
						.Format(rawSize, rawSizeWidth, TFileDumper.Align.Center), TFileDumper.Align.Format
						(compression, compressionWidth, TFileDumper.Align.Left));
					for (IEnumerator<KeyValuePair<string, BCFile.MetaIndexEntry>> it_3 = metaBlkEntrySet
						.GetEnumerator(); it_3.HasNext(); )
					{
						KeyValuePair<string, BCFile.MetaIndexEntry> e = it_3.Next();
						string blkName = e.Value.GetMetaName();
						BCFile.BlockRegion region = e.Value.GetRegion();
						string blkCompression = e.Value.GetCompressionAlgorithm().GetName();
						@out.Printf("%s %s %s %s %s%n", TFileDumper.Align.Format(blkName, nameWidth, TFileDumper.Align
							.Left), TFileDumper.Align.Format(region.GetOffset(), offsetWidth, TFileDumper.Align
							.Left), TFileDumper.Align.Format(region.GetCompressedSize(), blkLenWidth, TFileDumper.Align
							.Left), TFileDumper.Align.Format(region.GetRawSize(), rawSizeWidth, TFileDumper.Align
							.Left), TFileDumper.Align.Format(blkCompression, compressionWidth, TFileDumper.Align
							.Left));
					}
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, reader, fsdis);
			}
		}
	}
}
