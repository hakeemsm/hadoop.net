using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>Dumping the information of a TFile.</summary>
	internal class TFileDumper
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.file.tfile.TFileDumper
			)));

		private TFileDumper()
		{
		}

		[System.Serializable]
		private sealed class Align
		{
			public static readonly org.apache.hadoop.io.file.tfile.TFileDumper.Align LEFT = new 
				org.apache.hadoop.io.file.tfile.TFileDumper.Align();

			public static readonly org.apache.hadoop.io.file.tfile.TFileDumper.Align CENTER = 
				new org.apache.hadoop.io.file.tfile.TFileDumper.Align();

			public static readonly org.apache.hadoop.io.file.tfile.TFileDumper.Align RIGHT = 
				new org.apache.hadoop.io.file.tfile.TFileDumper.Align();

			public static readonly org.apache.hadoop.io.file.tfile.TFileDumper.Align ZERO_PADDED
				 = new org.apache.hadoop.io.file.tfile.TFileDumper.Align();

			// namespace object not constructable.
			internal static string format(string s, int width, org.apache.hadoop.io.file.tfile.TFileDumper.Align
				 align)
			{
				if (s.Length >= width)
				{
					return s;
				}
				int room = width - s.Length;
				org.apache.hadoop.io.file.tfile.TFileDumper.Align alignAdjusted = align;
				if (room == 1)
				{
					alignAdjusted = org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT;
				}
				if (alignAdjusted == org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT)
				{
					return s + string.format("%" + room + "s", string.Empty);
				}
				if (alignAdjusted == org.apache.hadoop.io.file.tfile.TFileDumper.Align.RIGHT)
				{
					return string.format("%" + room + "s", string.Empty) + s;
				}
				if (alignAdjusted == org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER)
				{
					int half = room / 2;
					return string.format("%" + half + "s", string.Empty) + s + string.format("%" + (room
						 - half) + "s", string.Empty);
				}
				throw new System.ArgumentException("Unsupported alignment");
			}

			internal static string format(long l, int width, org.apache.hadoop.io.file.tfile.TFileDumper.Align
				 align)
			{
				if (align == org.apache.hadoop.io.file.tfile.TFileDumper.Align.ZERO_PADDED)
				{
					return string.format("%0" + width + "d", l);
				}
				return format(System.Convert.ToString(l), width, align);
			}

			internal static int calculateWidth(string caption, long max)
			{
				return System.Math.max(caption.Length, System.Convert.ToString(max).Length);
			}
		}

		/// <summary>Dump information about TFile.</summary>
		/// <param name="file">Path string of the TFile</param>
		/// <param name="out">PrintStream to output the information.</param>
		/// <param name="conf">The configuration object.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void dumpInfo(string file, System.IO.TextWriter @out, org.apache.hadoop.conf.Configuration
			 conf)
		{
			int maxKeySampleLen = 16;
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file);
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
			long length = fs.getFileStatus(path).getLen();
			org.apache.hadoop.fs.FSDataInputStream fsdis = fs.open(path);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fsdis, length, conf);
			try
			{
				java.util.LinkedHashMap<string, string> properties = new java.util.LinkedHashMap<
					string, string>();
				int blockCnt = reader.readerBCF.getBlockCount();
				int metaBlkCnt = reader.readerBCF.metaIndex.index.Count;
				properties["BCFile Version"] = reader.readerBCF.version.ToString();
				properties["TFile Version"] = reader.tfileMeta.version.ToString();
				properties["File Length"] = System.Convert.ToString(length);
				properties["Data Compression"] = reader.readerBCF.getDefaultCompressionName();
				properties["Record Count"] = System.Convert.ToString(reader.getEntryCount());
				properties["Sorted"] = bool.toString(reader.isSorted());
				if (reader.isSorted())
				{
					properties["Comparator"] = reader.getComparatorName();
				}
				properties["Data Block Count"] = int.toString(blockCnt);
				long dataSize = 0;
				long dataSizeUncompressed = 0;
				if (blockCnt > 0)
				{
					for (int i = 0; i < blockCnt; ++i)
					{
						org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region = reader.readerBCF.dataIndex
							.getBlockRegionList()[i];
						dataSize += region.getCompressedSize();
						dataSizeUncompressed += region.getRawSize();
					}
					properties["Data Block Bytes"] = System.Convert.ToString(dataSize);
					if (reader.readerBCF.getDefaultCompressionName() != "none")
					{
						properties["Data Block Uncompressed Bytes"] = System.Convert.ToString(dataSizeUncompressed
							);
						properties["Data Block Compression Ratio"] = string.format("1:%.1f", (double)dataSizeUncompressed
							 / dataSize);
					}
				}
				properties["Meta Block Count"] = int.toString(metaBlkCnt);
				long metaSize = 0;
				long metaSizeUncompressed = 0;
				if (metaBlkCnt > 0)
				{
					System.Collections.Generic.ICollection<org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
						> metaBlks = reader.readerBCF.metaIndex.index.Values;
					bool calculateCompression = false;
					for (System.Collections.Generic.IEnumerator<org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
						> it = metaBlks.GetEnumerator(); it.MoveNext(); )
					{
						org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry e = it.Current;
						metaSize += e.getRegion().getCompressedSize();
						metaSizeUncompressed += e.getRegion().getRawSize();
						if (e.getCompressionAlgorithm() != org.apache.hadoop.io.file.tfile.Compression.Algorithm
							.NONE)
						{
							calculateCompression = true;
						}
					}
					properties["Meta Block Bytes"] = System.Convert.ToString(metaSize);
					if (calculateCompression)
					{
						properties["Meta Block Uncompressed Bytes"] = System.Convert.ToString(metaSizeUncompressed
							);
						properties["Meta Block Compression Ratio"] = string.format("1:%.1f", (double)metaSizeUncompressed
							 / metaSize);
					}
				}
				properties["Meta-Data Size Ratio"] = string.format("1:%.1f", (double)dataSize / metaSize
					);
				long leftOverBytes = length - dataSize - metaSize;
				long miscSize = org.apache.hadoop.io.file.tfile.BCFile.Magic.size() * 2 + long.SIZE
					 / byte.SIZE + org.apache.hadoop.io.file.tfile.Utils.Version.size();
				long metaIndexSize = leftOverBytes - miscSize;
				properties["Meta Block Index Bytes"] = System.Convert.ToString(metaIndexSize);
				properties["Headers Etc Bytes"] = System.Convert.ToString(miscSize);
				// Now output the properties table.
				int maxKeyLength = 0;
				System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<string
					, string>> entrySet = properties;
				for (System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair
					<string, string>> it_1 = entrySet.GetEnumerator(); it_1.MoveNext(); )
				{
					System.Collections.Generic.KeyValuePair<string, string> e = it_1.Current;
					if (e.Key.Length > maxKeyLength)
					{
						maxKeyLength = e.Key.Length;
					}
				}
				for (System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair
					<string, string>> it_2 = entrySet.GetEnumerator(); it_2.MoveNext(); )
				{
					System.Collections.Generic.KeyValuePair<string, string> e = it_2.Current;
					@out.printf("%s : %s%n", org.apache.hadoop.io.file.tfile.TFileDumper.Align.format
						(e.Key, maxKeyLength, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT), e
						.Value);
				}
				@out.WriteLine();
				reader.checkTFileDataIndex();
				if (blockCnt > 0)
				{
					string blkID = "Data-Block";
					int blkIDWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(blkID, blockCnt);
					int blkIDWidth2 = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(string.Empty, blockCnt);
					string offset = "Offset";
					int offsetWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(offset, length);
					string blkLen = "Length";
					int blkLenWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(blkLen, dataSize / blockCnt * 10);
					string rawSize = "Raw-Size";
					int rawSizeWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(rawSize, dataSizeUncompressed / blockCnt * 10);
					string records = "Records";
					int recordsWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(records, reader.getEntryCount() / blockCnt * 10);
					string endKey = "End-Key";
					int endKeyWidth = System.Math.max(endKey.Length, maxKeySampleLen * 2 + 5);
					@out.printf("%s %s %s %s %s %s%n", org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.format(blkID, blkIDWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER
						), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(offset, offsetWidth, 
						org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER), org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.format(blkLen, blkLenWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER
						), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(rawSize, rawSizeWidth
						, org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER), org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.format(records, recordsWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.CENTER), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(endKey, endKeyWidth
						, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT));
					for (int i = 0; i < blockCnt; ++i)
					{
						org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region = reader.readerBCF.dataIndex
							.getBlockRegionList()[i];
						org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry indexEntry = reader.tfileIndex
							.getEntry(i);
						@out.printf("%s %s %s %s %s ", org.apache.hadoop.io.file.tfile.TFileDumper.Align.
							format(org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(i, blkIDWidth2, 
							org.apache.hadoop.io.file.tfile.TFileDumper.Align.ZERO_PADDED), blkIDWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(region.getOffset
							(), offsetWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.format(region.getCompressedSize(), blkLenWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(region.getRawSize
							(), rawSizeWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.format(indexEntry.kvEntries, recordsWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.LEFT));
						byte[] key = indexEntry.key;
						bool asAscii = true;
						int sampleLen = System.Math.min(maxKeySampleLen, key.Length);
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
								@out.printf("%X", b);
							}
						}
						else
						{
							@out.Write(new string(key, 0, sampleLen, org.apache.commons.io.Charsets.UTF_8));
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
					System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<string
						, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry>> metaBlkEntrySet = reader
						.readerBCF.metaIndex.index;
					for (System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair
						<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry>> it = metaBlkEntrySet
						.GetEnumerator(); it_2.MoveNext(); )
					{
						System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
							> e = it_2.Current;
						if (e.Key.Length > maxNameLen)
						{
							maxNameLen = e.Key.Length;
						}
					}
					int nameWidth = System.Math.max(name.Length, maxNameLen);
					string offset = "Offset";
					int offsetWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(offset, length);
					string blkLen = "Length";
					int blkLenWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(blkLen, metaSize / metaBlkCnt * 10);
					string rawSize = "Raw-Size";
					int rawSizeWidth = org.apache.hadoop.io.file.tfile.TFileDumper.Align.calculateWidth
						(rawSize, metaSizeUncompressed / metaBlkCnt * 10);
					string compression = "Compression";
					int compressionWidth = compression.Length;
					@out.printf("%s %s %s %s %s%n", org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.format(name, nameWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER
						), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(offset, offsetWidth, 
						org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER), org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.format(blkLen, blkLenWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER
						), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(rawSize, rawSizeWidth
						, org.apache.hadoop.io.file.tfile.TFileDumper.Align.CENTER), org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.format(compression, compressionWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
						.LEFT));
					for (System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair
						<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry>> it_3 = metaBlkEntrySet
						.GetEnumerator(); it_3.MoveNext(); )
					{
						System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
							> e = it_3.Current;
						string blkName = e.Value.getMetaName();
						org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region = e.Value.getRegion();
						string blkCompression = e.Value.getCompressionAlgorithm().getName();
						@out.printf("%s %s %s %s %s%n", org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.format(blkName, nameWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT
							), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(region.getOffset(), 
							offsetWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.format(region.getCompressedSize(), blkLenWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align.format(region.getRawSize
							(), rawSizeWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align.LEFT), org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.format(blkCompression, compressionWidth, org.apache.hadoop.io.file.tfile.TFileDumper.Align
							.LEFT));
					}
				}
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, reader, fsdis);
			}
		}
	}
}
