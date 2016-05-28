using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests of XAttr operations using FileContext APIs.</summary>
	public class TestFileContextXAttr : FSXAttrBaseTest
	{
		/// <exception cref="System.Exception"/>
		protected internal override FileSystem CreateFileSystem()
		{
			TestFileContextXAttr.FileContextFS fcFs = new TestFileContextXAttr.FileContextFS(
				);
			fcFs.Initialize(FileSystem.GetDefaultUri(conf), conf);
			return fcFs;
		}

		/// <summary>
		/// This reuses FSXAttrBaseTest's testcases by creating a filesystem
		/// implementation which uses FileContext by only overriding the xattr related
		/// methods.
		/// </summary>
		/// <remarks>
		/// This reuses FSXAttrBaseTest's testcases by creating a filesystem
		/// implementation which uses FileContext by only overriding the xattr related
		/// methods. Other operations will use the normal filesystem.
		/// </remarks>
		public class FileContextFS : DistributedFileSystem
		{
			private FileContext fc;

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI uri, Configuration conf)
			{
				base.Initialize(uri, conf);
				fc = FileContext.GetFileContext(conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetXAttr(Path path, string name, byte[] value)
			{
				fc.SetXAttr(path, name, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
				> flag)
			{
				fc.SetXAttr(path, name, value, flag);
			}

			/// <exception cref="System.IO.IOException"/>
			public override byte[] GetXAttr(Path path, string name)
			{
				return fc.GetXAttr(path, name);
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, byte[]> GetXAttrs(Path path)
			{
				return fc.GetXAttrs(path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
				)
			{
				return fc.GetXAttrs(path, names);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveXAttr(Path path, string name)
			{
				fc.RemoveXAttr(path, name);
			}
		}
	}
}
